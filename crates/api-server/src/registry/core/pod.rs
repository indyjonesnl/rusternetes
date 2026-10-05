//! Pod strategies and storage — port of `pkg/registry/core/pod/strategy.go`
//! (`podStrategy`, `podEphemeralContainersStrategy`, `podResizeStrategy`) and
//! `pkg/registry/core/pod/storage/storage.go` (`NewStorage`).
//!
//! Not modelled here, each tracked in #1990's follow-ups:
//!
//! * `podutil.DropDisabledPodFields`: every gate it covers is either on by
//!   default in 1.35 or has no field in our types.
//! * `applySchedulingGatedCondition`, `mutatePodAffinity`,
//!   `mutateTopologySpreadConstraints` and `applyAppArmorVersionSkew`
//!   (strategy.go:92-97): the api-server never ran them.
//! * `podutil.DropDisabledPodFields` on `/status` (as above).

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use async_trait::async_trait;

use rusternetes_common::equality::semantic_equal;
use rusternetes_common::podutil::update_pod_condition;
use rusternetes_common::resources::pod::PodSpec;
use rusternetes_common::resources::{Binding, Container, Pod, PodCondition, PodStatus};
use rusternetes_common::types::Phase;
use rusternetes_common::validation::field::{Error as FieldError, ErrorList, Path};
use rusternetes_common::validation::metav1::{get_warnings_for_ip, is_dns1123_label};
use rusternetes_common::validation::objectmeta::{
    name_is_dns_subdomain, validate_object_meta, validate_object_meta_update,
};
use rusternetes_common::Status;
use rusternetes_storage::StorageBackend;

use crate::registry::generic::{Store, UpdateOptions};
use crate::registry::rest::{
    reset_object_meta_for_status, DefaultUpdatedObjectInfo, GroupResource, NamespaceScopedStrategy,
    RequestContext, RestCreateStrategy, RestDeleteStrategy, RestGracefulDeleteStrategy,
    RestUpdateStrategy, TransformFunc, UpdatedObjectInfo,
};

/// `api.MirrorPodAnnotationKey`.
const MIRROR_POD_ANNOTATION_KEY: &str = "kubernetes.io/config.mirror";

/// What decoding a Pod does beyond the field mapping: `SetDefaults_PodSpec`,
/// `SetDefaults_Container` and `SetDefaults_Probe` (pkg/apis/core/v1/
/// defaults.go), and the Pod-only defaults (`enableServiceLinks`, limits to
/// requests). Upstream's codec applies them on every decode, before
/// `ValidatePod` sees the object.
pub fn convert_to_internal(pod: &mut Pod) {
    if let Some(spec) = pod.spec.as_mut() {
        crate::handlers::defaults::apply_pod_spec_defaults(spec);
        crate::handlers::defaults::apply_pod_defaults(spec);
    }
}

/// `ValidatePod`'s ObjectMeta half (validation.go:4986-4990): the name is a
/// DNS subdomain (`ValidatePodName`).
fn validate_pod_meta(pod: &Pod) -> ErrorList {
    validate_object_meta(
        &pod.metadata,
        true,
        name_is_dns_subdomain,
        &Path::new("metadata"),
    )
}

/// `applySchedulingGatedCondition` (strategy.go:929-947): a new pod with
/// scheduling gates and no `PodScheduled` condition gets
/// `PodScheduled=False, reason SchedulingGated`.
fn apply_scheduling_gated_condition(pod: &mut Pod) {
    let gated = pod
        .spec
        .as_ref()
        .is_some_and(|s| s.scheduling_gates.as_ref().is_some_and(|g| !g.is_empty()));
    if !gated {
        return;
    }
    let status = pod.status.get_or_insert_with(Default::default);
    if status
        .conditions
        .iter()
        .flatten()
        .any(|c| c.condition_type == "PodScheduled")
    {
        return;
    }
    update_pod_condition(
        status,
        PodCondition {
            condition_type: "PodScheduled".to_string(),
            status: "False".to_string(),
            reason: Some("SchedulingGated".to_string()),
            message: Some("Scheduling is blocked due to non-empty scheduling gates".to_string()),
            last_probe_time: None,
            last_transition_time: None,
            observed_generation: None,
        },
    );
}

/// `updatePodGeneration` (strategy.go:230-236): the generation moves when the
/// spec does.
fn update_pod_generation(new: &mut Pod, old: &Pod) {
    if !semantic_equal(&new.spec, &old.spec) {
        new.metadata.generation = Some(old.metadata.generation.unwrap_or(0) + 1);
    }
}

/// `ValidatePodUpdate` (validation.go:5695-5836) for the spec fields the
/// update fence guards; the fence itself is shared with the other pod
/// validators in `common`.
fn validate_spec_update(old: &PodSpec, new: &PodSpec, ephemeral: bool) -> ErrorList {
    rusternetes_common::validation::pod::validate_pod_spec_update(old, new, ephemeral)
}

/// `ValidatePodUpdate`'s `spec.nodeName` rule: once bound, a pod's node may
/// not change (the binding subresource is the only writer).
fn validate_node_name_immutable(old: &PodSpec, new: &PodSpec) -> ErrorList {
    let old_node = old.node_name.as_deref().unwrap_or("");
    let new_node = new.node_name.as_deref().unwrap_or("");
    if !old_node.is_empty() && new_node != old_node {
        return vec![FieldError::forbidden(
            &Path::new("spec").child("nodeName"),
            "field is immutable",
        )];
    }
    Vec::new()
}

/// `podStrategy` (strategy.go:60-72).
pub struct Strategy;

impl NamespaceScopedStrategy for Strategy {
    fn namespace_scoped(&self) -> bool {
        true
    }
}

impl RestCreateStrategy<Pod> for Strategy {
    /// strategy.go:84-100: a new pod is `Pending` with its QoS class, at
    /// generation 1, whatever the client sent as status.
    fn prepare_for_create(&self, _ctx: &RequestContext, obj: &mut Pod) {
        obj.metadata.generation = Some(1);
        // The api-server is the authoritative writer of the QoS class
        // (strategy.go:92); the kubelet recomputes the same value
        // (pkg/kubelet/kubelet_pods.go:2097), both through `ComputePodQOS`.
        let qos = rusternetes_common::qos::compute_pod_qos(obj);
        obj.status = Some(PodStatus {
            phase: Some(Phase::Pending),
            qos_class: Some(qos.as_str().to_string()),
            ..Default::default()
        });
        rusternetes_common::pod_drop_disabled::drop_disabled_pod_fields(obj, None);
        apply_scheduling_gated_condition(obj);
    }

    /// strategy.go:111-116: `ValidatePodCreate`.
    fn validate(&self, _ctx: &RequestContext, obj: &Pod) -> ErrorList {
        use rusternetes_common::feature_gates::{enabled, Feature};
        let mut errs = validate_pod_meta(obj);
        errs.extend(rusternetes_common::validation::pod::validate_pod_create(
            obj,
            enabled(Feature::RelaxedDNSSearchValidation),
        ));
        errs
    }

    /// strategy.go:119-128. `GetWarningsForPod` is not ported.
    fn warnings_on_create(&self, _ctx: &RequestContext, obj: &Pod) -> Vec<String> {
        let msgs = is_dns1123_label(&obj.metadata.name);
        if msgs.is_empty() {
            return rusternetes_common::pod_warnings::get_warnings_for_pod(obj, None);
        }
        let mut warnings = vec![format!(
            "metadata.name: this is used in the Pod's hostname, which can result in surprising behavior; a DNS label is recommended: [{}]",
            msgs.join(" ")
        )];
        warnings.extend(rusternetes_common::pod_warnings::get_warnings_for_pod(
            obj, None,
        ));
        warnings
    }
}

impl RestUpdateStrategy<Pod> for Strategy {
    /// strategy.go:138-140.
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// strategy.go:103-109: the main resource never writes status.
    fn prepare_for_update(&self, _ctx: &RequestContext, obj: &mut Pod, old: &Pod) {
        obj.status = old.status.clone();
        rusternetes_common::pod_drop_disabled::drop_disabled_pod_fields(obj, Some(old));
        update_pod_generation(obj, old);
    }

    /// strategy.go:143-150: `ValidatePodUpdate`.
    fn validate_update(&self, _ctx: &RequestContext, obj: &Pod, old: &Pod) -> ErrorList {
        let mut errs =
            validate_object_meta_update(&obj.metadata, &old.metadata, &Path::new("metadata"));
        if let (Some(old_spec), Some(new_spec)) = (old.spec.as_ref(), obj.spec.as_ref()) {
            errs.extend(validate_node_name_immutable(old_spec, new_spec));
            errs.extend(validate_spec_update(old_spec, new_spec, false));
        }
        errs
    }

    /// strategy.go:153-157: no warnings on pod update.
    fn warnings_on_update(&self, _ctx: &RequestContext, _obj: &Pod, _old: &Pod) -> Vec<String> {
        Vec::new()
    }

    /// strategy.go:160-163.
    fn allow_unconditional_update(&self) -> bool {
        true
    }
}

impl RestDeleteStrategy<Pod> for Strategy {
    fn graceful(&self) -> Option<&dyn RestGracefulDeleteStrategy<Pod>> {
        Some(self)
    }
}

impl RestGracefulDeleteStrategy<Pod> for Strategy {
    /// `CheckGracefulDelete` (strategy.go:166-197).
    fn check_graceful_delete(
        &self,
        _ctx: &RequestContext,
        obj: &Pod,
        options: &mut rusternetes_common::deletion::DeleteOptions,
    ) -> bool {
        let spec = obj.spec.as_ref();
        // user has specified a value; otherwise use the default if set, or
        // delete the pod immediately (0)
        let mut period = match options.grace_period_seconds {
            Some(p) => p,
            None => spec
                .and_then(|s| s.termination_grace_period_seconds)
                .unwrap_or(0),
        };
        // if the pod is not scheduled, delete immediately
        if spec
            .and_then(|s| s.node_name.as_deref())
            .unwrap_or("")
            .is_empty()
        {
            period = 0;
        }
        // if the pod is already terminated, delete immediately
        let phase = obj.status.as_ref().and_then(|s| s.phase.as_ref());
        if matches!(phase, Some(Phase::Failed) | Some(Phase::Succeeded)) {
            period = 0;
        }
        if period < 0 {
            period = 1;
        }
        // ensure the options and the pod are in sync
        options.grace_period_seconds = Some(period);
        true
    }
}

/// `podStatusStrategy` (strategy.go:197-283): the update strategy of
/// `/status`.
pub struct StatusStrategy;

impl NamespaceScopedStrategy for StatusStrategy {
    fn namespace_scoped(&self) -> bool {
        true
    }
}

/// `preserveOldObservedGeneration` (strategy.go:237-261): a request that
/// clears `observedGeneration`, in the status or in a condition, keeps the
/// stored value. Go's zero is "unset", so `None` and `0` are the same here.
fn preserve_old_observed_generation(new: &mut Pod, old: &Pod) {
    let old_status = old.status.clone().unwrap_or_default();
    let new_status = new.status.get_or_insert_with(Default::default);
    if new_status.observed_generation.unwrap_or(0) == 0 {
        new_status.observed_generation = old_status.observed_generation;
    }

    // Remember observedGeneration values from old status conditions. This is
    // a list per type because validation permits multiple conditions with the
    // same type.
    let mut old_generations: HashMap<String, VecDeque<i64>> = HashMap::new();
    for condition in old_status.conditions.as_deref().unwrap_or(&[]) {
        old_generations
            .entry(condition.condition_type.clone())
            .or_default()
            .push_back(condition.observed_generation.unwrap_or(0));
    }

    // For any conditions in the new status without observedGeneration set,
    // preserve the old value.
    for condition in new_status.conditions.iter_mut().flatten() {
        let old_generation = old_generations
            .get_mut(&condition.condition_type)
            .and_then(|generations| generations.pop_front())
            .unwrap_or(0);
        if condition.observed_generation.unwrap_or(0) == 0 {
            condition.observed_generation = (old_generation != 0).then_some(old_generation);
        }
    }
}

impl RestUpdateStrategy<Pod> for StatusStrategy {
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// strategy.go:216-235. `DropDisabledPodFields` is not modelled (module
    /// doc).
    fn prepare_for_update(&self, _ctx: &RequestContext, obj: &mut Pod, old: &Pod) {
        obj.spec = old.spec.clone();
        obj.metadata.deletion_timestamp = None;

        // don't allow the pods/status endpoint to touch owner references
        // since old kubelets corrupt them in a way that breaks garbage
        // collection
        obj.metadata.owner_references = old.metadata.owner_references.clone();
        // the Pod QoS is immutable and populated at creation time by the
        // kube-apiserver. we need to backfill it for backward compatibility
        // because the old kubelet dropped this field when the pod was
        // rejected.
        let old_qos = old.status.as_ref().and_then(|s| s.qos_class.clone());
        let status = obj.status.get_or_insert_with(Default::default);
        if status.qos_class.as_deref().unwrap_or("").is_empty() {
            status.qos_class = old_qos;
        }

        preserve_old_observed_generation(obj, old);
        rusternetes_common::pod_drop_disabled::drop_disabled_pod_fields(obj, Some(old));
    }

    /// strategy.go:263-271: `ValidatePodStatusUpdate`.
    fn validate_update(&self, _ctx: &RequestContext, obj: &Pod, old: &Pod) -> ErrorList {
        rusternetes_common::validation::pod_status::validate_pod_status_update(obj, old)
    }

    /// strategy.go:273-283: a non-standard IP in `podIPs` or `hostIPs`
    /// draws a warning.
    fn warnings_on_update(&self, _ctx: &RequestContext, obj: &Pod, _old: &Pod) -> Vec<String> {
        let Some(status) = obj.status.as_ref() else {
            return Vec::new();
        };
        let mut warnings = Vec::new();
        for (i, pod_ip) in status.pod_i_ps.iter().flatten().enumerate() {
            warnings.extend(get_warnings_for_ip(
                &format!("status.podIPs[{i}].ip"),
                &pod_ip.ip,
            ));
        }
        for (i, host_ip) in status.host_i_ps.iter().flatten().enumerate() {
            warnings.extend(get_warnings_for_ip(
                &format!("status.hostIPs[{i}].ip"),
                &host_ip.ip,
            ));
        }
        warnings
    }

    fn allow_unconditional_update(&self) -> bool {
        true
    }
}

/// `podEphemeralContainersStrategy` (strategy.go:300-355): the update
/// strategy of `/ephemeralcontainers`.
pub struct EphemeralContainersStrategy;

impl NamespaceScopedStrategy for EphemeralContainersStrategy {
    fn namespace_scoped(&self) -> bool {
        true
    }
}

impl RestUpdateStrategy<Pod> for EphemeralContainersStrategy {
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// `dropNonEphemeralContainerUpdates` (strategy.go:314-322) and
    /// `updatePodGeneration`: only `spec.ephemeralContainers` may change.
    fn prepare_for_update(&self, _ctx: &RequestContext, obj: &mut Pod, old: &Pod) {
        let ephemeral = obj
            .spec
            .as_ref()
            .and_then(|s| s.ephemeral_containers.clone());
        obj.spec = old.spec.clone();
        if let Some(spec) = obj.spec.as_mut() {
            spec.ephemeral_containers = ephemeral;
        }
        obj.status = old.status.clone();
        reset_object_meta_for_status(&mut obj.metadata, &old.metadata);
        rusternetes_common::pod_drop_disabled::drop_disabled_pod_fields(obj, Some(old));
        update_pod_generation(obj, old);
    }

    /// `ValidatePodEphemeralContainersUpdate` (validation.go:6181-6212):
    /// existing ephemeral containers may be neither removed nor changed.
    fn validate_update(&self, _ctx: &RequestContext, obj: &Pod, old: &Pod) -> ErrorList {
        let mut errs =
            validate_object_meta_update(&obj.metadata, &old.metadata, &Path::new("metadata"));
        // static pods don't support ephemeral containers #113935
        if old
            .metadata
            .annotations
            .as_ref()
            .is_some_and(|a| a.contains_key(MIRROR_POD_ANNOTATION_KEY))
        {
            return vec![FieldError::forbidden(
                &Path::new(""),
                "static pods do not support ephemeral containers",
            )];
        }
        let new_containers = obj
            .spec
            .as_ref()
            .and_then(|s| s.ephemeral_containers.as_deref())
            .unwrap_or(&[]);
        let old_containers = old
            .spec
            .as_ref()
            .and_then(|s| s.ephemeral_containers.as_deref())
            .unwrap_or(&[]);
        let spec_path = Path::new("spec").child("ephemeralContainers");
        for old_container in old_containers {
            match new_containers.iter().find(|c| c.name == old_container.name) {
                None => errs.push(FieldError::forbidden(
                    &spec_path,
                    format!(
                        "existing ephemeral containers {:?} may not be removed\n",
                        old_container.name
                    ),
                )),
                Some(new_container) if !semantic_equal(old_container, new_container) => {
                    errs.push(FieldError::forbidden(
                        &spec_path,
                        format!(
                            "existing ephemeral containers {:?} may not be changed\n",
                            old_container.name
                        ),
                    ))
                }
                Some(_) => {}
            }
        }
        errs
    }

    fn allow_unconditional_update(&self) -> bool {
        true
    }
}

/// `podResizeStrategy` (strategy.go:357-): the update strategy of `/resize`.
pub struct ResizeStrategy;

impl NamespaceScopedStrategy for ResizeStrategy {
    fn namespace_scoped(&self) -> bool {
        true
    }
}

/// `dropNonResizeUpdatesForContainers` (strategy.go:437-460): the old
/// containers with the new `resources` and `resizePolicy`. A reordered or
/// renamed container is returned as-is, for validation to refuse.
fn drop_non_resize_updates_for_containers(new: &[Container], old: &[Container]) -> Vec<Container> {
    if new.is_empty() {
        return new.to_vec();
    }
    let mut merged = old.to_vec();
    for (i, ctr) in new.iter().enumerate() {
        if merged[i].name != ctr.name {
            return new.to_vec();
        }
        merged[i].resources = ctr.resources.clone();
        merged[i].resize_policy = ctr.resize_policy.clone();
    }
    merged
}

/// `dropNonResizeUpdates` (strategy.go:386-435), without the pod-level
/// resources of the alpha `InPlacePodLevelResourcesVerticalScaling` gate.
fn drop_non_resize_updates(new: &mut Pod, old: &Pod) {
    let (Some(new_spec), Some(old_spec)) = (new.spec.as_ref(), old.spec.as_ref()) else {
        return;
    };
    // Containers are not allowed to be added, removed, re-ordered, or
    // renamed. If we detect any of these changes, the new podspec is kept
    // as-is and validation catches the error.
    let new_init = new_spec.init_containers.as_deref().unwrap_or(&[]);
    let old_init = old_spec.init_containers.as_deref().unwrap_or(&[]);
    if new_spec.containers.len() != old_spec.containers.len() || new_init.len() != old_init.len() {
        return;
    }
    let containers =
        drop_non_resize_updates_for_containers(&new_spec.containers, &old_spec.containers);
    let init_containers = drop_non_resize_updates_for_containers(new_init, old_init);

    let mut spec = old_spec.clone();
    spec.containers = containers;
    if spec.init_containers.is_some() {
        spec.init_containers = Some(init_containers);
    }
    new.spec = Some(spec);
    new.status = old.status.clone();
    reset_object_meta_for_status(&mut new.metadata, &old.metadata);
}

/// Whether a container's compute resources differ, the way
/// `apiequality.Semantic.DeepEqual` sees them: an unset resources block and
/// one with empty maps are the same (a Go client re-marshals `"resources":{}`).
fn container_resources_changed(old: &Pod, new: &Pod) -> bool {
    let containers = |p: &Pod| p.spec.as_ref().map(|s| s.containers.clone());
    let (Some(old_cs), Some(new_cs)) = (containers(old), containers(new)) else {
        return false;
    };
    new_cs.iter().any(|new_c| {
        old_cs
            .iter()
            .find(|c| c.name == new_c.name)
            .is_some_and(|old_c| !semantic_equal(&old_c.resources, &new_c.resources))
    })
}

impl RestUpdateStrategy<Pod> for ResizeStrategy {
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// strategy.go `podResizeStrategy.PrepareForUpdate`, plus the legacy
    /// `status.resize = Proposed` the kubelet reads (KEP-1287).
    fn prepare_for_update(&self, _ctx: &RequestContext, obj: &mut Pod, old: &Pod) {
        drop_non_resize_updates(obj, old);
        rusternetes_common::pod_drop_disabled::drop_disabled_pod_fields(obj, Some(old));
        update_pod_generation(obj, old);
        if container_resources_changed(old, obj) {
            if let Some(status) = obj.status.as_mut() {
                status.resize = Some("Proposed".to_string());
            }
        }
    }

    /// `ValidatePodResize` (validation.go:6217-): only `resources` and
    /// `resizePolicy` of the containers may differ, so those are reset to the
    /// old values before the update fence compares the specs.
    fn validate_update(&self, _ctx: &RequestContext, obj: &Pod, old: &Pod) -> ErrorList {
        let mut errs =
            validate_object_meta_update(&obj.metadata, &old.metadata, &Path::new("metadata"));
        if old
            .metadata
            .annotations
            .as_ref()
            .is_some_and(|a| a.contains_key(MIRROR_POD_ANNOTATION_KEY))
        {
            return vec![FieldError::forbidden(
                &Path::new(""),
                "static pods cannot be resized",
            )];
        }
        if let (Some(old_spec), Some(new_spec)) = (old.spec.as_ref(), obj.spec.as_ref()) {
            let mut munged = new_spec.clone();
            for (i, c) in munged.containers.iter_mut().enumerate() {
                if let Some(o) = old_spec.containers.get(i) {
                    c.resources = o.resources.clone();
                    c.resize_policy = o.resize_policy.clone();
                }
            }
            if let (Some(old_init), Some(new_init)) = (
                old_spec.init_containers.as_ref(),
                munged.init_containers.as_mut(),
            ) {
                for (i, c) in new_init.iter_mut().enumerate() {
                    if let Some(o) = old_init.get(i) {
                        c.resources = o.resources.clone();
                        c.resize_policy = o.resize_policy.clone();
                    }
                }
            }
            errs.extend(validate_spec_update(old_spec, &munged, false));
        }
        errs
    }

    fn allow_unconditional_update(&self) -> bool {
        true
    }
}

/// `NewStorage` (storage/storage.go:63-): the Pod store.
pub fn new_store(storage: Arc<StorageBackend>) -> Store<Pod, StorageBackend> {
    let mut store = Store::new(storage, GroupResource::new("", "pods"), Arc::new(Strategy))
        .with_decode_defaulter(convert_to_internal);
    store.return_deleted_object = true;
    store
}

/// The `/status` store (storage.go: `StatusREST`, whose store carries
/// `registrypod.StatusStrategy`). The eviction subresource writes through the
/// same store.
pub fn new_status_store(storage: Arc<StorageBackend>) -> Store<Pod, StorageBackend> {
    new_store(storage).with_update_strategy(Arc::new(StatusStrategy))
}

/// The `/ephemeralcontainers` store (storage.go: `EphemeralContainersREST`).
pub fn new_ephemeral_containers_store(storage: Arc<StorageBackend>) -> Store<Pod, StorageBackend> {
    new_store(storage).with_update_strategy(Arc::new(EphemeralContainersStrategy))
}

/// The `/resize` store (storage.go: `ResizeREST`).
pub fn new_resize_store(storage: Arc<StorageBackend>) -> Store<Pod, StorageBackend> {
    new_store(storage).with_update_strategy(Arc::new(ResizeStrategy))
}

/// `BindingREST` (storage/storage.go:149-297, `Create` and its
/// `assignPod` / `setPodNodeAndMetadata`): binds a pod to a node by writing
/// the pod straight through the storage with the binding's UID and
/// resourceVersion as preconditions, around the pod strategies.
pub struct BindingRest {
    store: Store<Pod, StorageBackend>,
}

impl BindingRest {
    pub fn new(storage: Arc<StorageBackend>) -> Self {
        Self {
            store: new_store(storage),
        }
    }

    /// `BindingREST.Create` (storage.go:177-201), after the handler decoded
    /// `binding` and the admission chain mutated it.
    pub async fn create(
        &self,
        ctx: &RequestContext,
        name: &str,
        binding: &Binding,
        dry_run: bool,
    ) -> rusternetes_common::Result<()> {
        use rusternetes_common::Error;
        if name != binding.metadata.name {
            return Err(Error::BadRequest(
                "name in URL does not match name in Binding object".to_string(),
            ));
        }
        // TODO upstream: "move me to a binding strategy". An aggregate error
        // is a 500 there; the 422 with causes is kept (#1939).
        let errs = rusternetes_common::validation::pod_status::validate_pod_binding(binding);
        if !errs.is_empty() {
            return Err(Error::Invalid(errs));
        }
        self.assign_pod(ctx, binding, dry_run).await
    }

    /// `assignPod` (storage.go:286-296): any failure that is not already an
    /// API status is a Conflict on `pods/binding`.
    async fn assign_pod(
        &self,
        ctx: &RequestContext,
        binding: &Binding,
        dry_run: bool,
    ) -> rusternetes_common::Result<()> {
        let name = binding.metadata.name.as_str();
        // `PreserveRequestObjectMetaSystemFieldsOnSubresourceCreate`: the
        // binding's UID and resourceVersion guard the pod it names.
        let uid = (!binding.metadata.uid.is_empty()).then(|| binding.metadata.uid.clone());
        let resource_version = binding
            .metadata
            .resource_version
            .clone()
            .filter(|rv| !rv.is_empty());
        let preconditions = (uid.is_some() || resource_version.is_some()).then_some(
            rusternetes_common::deletion::Preconditions {
                uid,
                resource_version,
            },
        );
        let node = binding.target.name.clone();
        let annotations = binding.metadata.annotations.clone();
        let labels = binding.metadata.labels.clone();
        let binding_resource = GroupResource::new("", "pods/binding");
        let mutate = move |pod: &mut Pod| -> rusternetes_common::Result<()> {
            set_pod_node_and_metadata(pod, &node, &annotations, &labels)
                .map_err(|reason| crate::registry::rest::conflict(&binding_resource, name, reason))
        };
        self.store
            .guaranteed_update_checked(ctx, name, preconditions.as_ref(), dry_run, &mutate)
            .await
            .map(|_| ())
    }
}

/// The body of `setPodNodeAndMetadata`'s `SimpleUpdate` (storage.go:
/// 213-270): sets the node if and only if the pod is unassigned, and merges
/// the binding's annotations and labels.
fn set_pod_node_and_metadata(
    pod: &mut Pod,
    machine: &str,
    annotations: &Option<HashMap<String, String>>,
    labels: &Option<HashMap<String, String>>,
) -> Result<(), String> {
    use rusternetes_common::feature_gates::{enabled, Feature};
    let name = pod.metadata.name.clone();
    if pod.metadata.deletion_timestamp.is_some() {
        return Err(format!(
            "pod {name} is being deleted, cannot be assigned to a host"
        ));
    }
    let spec = pod.spec.get_or_insert_with(Default::default);
    if let Some(node) = spec.node_name.as_deref().filter(|n| !n.is_empty()) {
        return Err(format!("pod {name} is already assigned to node {node:?}"));
    }
    // Reject binding to a scheduling un-ready Pod.
    if spec
        .scheduling_gates
        .as_ref()
        .is_some_and(|g| !g.is_empty())
    {
        return Err(format!("pod {name} has non-empty .spec.schedulingGates"));
    }
    spec.node_name = Some(machine.to_string());
    // Clear nomination hint to prevent stale information affecting external
    // components (`ClearingNominatedNodeNameAfterBinding`, beta and on).
    let status = pod.status.get_or_insert_with(Default::default);
    status.nominated_node_name = None;
    if let Some(annotations) = annotations {
        pod.metadata
            .annotations
            .get_or_insert_with(Default::default)
            .extend(annotations.clone());
    }
    // Copy all labels from the Binding over to the Pod object, overwriting
    // any existing labels set on the Pod.
    if enabled(Feature::PodTopologyLabelsAdmission) {
        if let Some(labels) = labels.as_ref().filter(|l| !l.is_empty()) {
            pod.metadata
                .labels
                .get_or_insert_with(Default::default)
                .extend(labels.clone());
        }
    }
    update_pod_condition(
        pod.status.get_or_insert_with(Default::default),
        PodCondition {
            condition_type: "PodScheduled".to_string(),
            status: "True".to_string(),
            reason: None,
            message: None,
            last_probe_time: None,
            last_transition_time: None,
            observed_generation: None,
        },
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// EvictionREST (pkg/registry/core/pod/storage/eviction.go)
// ---------------------------------------------------------------------------

/// `MaxDisruptedPodSize` (eviction.go:54): the most entries
/// `PodDisruptionBudgetStatus.DisruptedPods` may hold before eviction refuses
/// to add to it.
pub const MAX_DISRUPTED_POD_SIZE: usize = 2000;

/// `wait.Backoff` (apimachinery/pkg/util/wait/backoff.go): `steps` is the
/// number of attempts, with a pause between them.
#[derive(Debug, Clone)]
pub struct Backoff {
    pub steps: u32,
    pub duration: std::time::Duration,
    pub factor: f64,
    pub jitter: f64,
}

/// `EvictionsRetry` (eviction.go:59-64): "the retry for a conflict where
/// multiple clients are making changes to the same resource".
pub fn evictions_retry() -> Backoff {
    Backoff {
        steps: 20,
        duration: std::time::Duration::from_millis(500),
        factor: 1.0,
        jitter: 0.1,
    }
}

impl Backoff {
    /// The pause `wait.ExponentialBackoff` takes between two attempts
    /// (backoff.go:`ExponentialBackoff`): `false`, without pausing, once the
    /// last attempt has been made (`if backoff.Steps == 1 { break }`).
    async fn wait(&mut self) -> bool {
        if self.steps <= 1 {
            return false;
        }
        self.steps -= 1;
        let mut delay = self.duration;
        if self.factor != 0.0 {
            self.duration = self.duration.mul_f64(self.factor);
        }
        if self.jitter > 0.0 {
            // `wait.Jitter`: duration + rand * maxFactor * duration.
            delay += delay.mul_f64(rand::random::<f64>() * self.jitter);
        }
        tokio::time::sleep(delay).await;
        true
    }
}

/// `errors.IsConflict`.
fn is_conflict(err: &rusternetes_common::Error) -> bool {
    matches!(err, rusternetes_common::Error::Conflict(_))
}

/// `dryrun.IsDryRun`: any entry is a dry run.
fn is_dry_run(dry_run: Option<&[String]>) -> bool {
    dry_run.is_some_and(|d| !d.is_empty())
}

/// `resourceVersionIsUnset` (eviction.go:409-411).
fn resource_version_is_unset(options: &rusternetes_common::deletion::DeleteOptions) -> bool {
    options
        .preconditions
        .as_ref()
        .is_none_or(|p| p.resource_version.is_none())
}

/// `setPreconditionsResourceVersion` (eviction.go:382-387).
fn set_preconditions_resource_version(
    options: &mut rusternetes_common::deletion::DeleteOptions,
    resource_version: Option<String>,
) {
    options
        .preconditions
        .get_or_insert_with(Default::default)
        .resource_version = Some(resource_version.unwrap_or_default());
}

/// `canIgnorePDB` (eviction.go:389-397): "pod conditions that allow the pod to
/// be deleted without checking PDBs".
fn can_ignore_pdb(pod: &Pod) -> bool {
    let phase = pod.status.as_ref().and_then(|s| s.phase.as_ref());
    matches!(
        phase,
        Some(Phase::Succeeded) | Some(Phase::Failed) | Some(Phase::Pending)
    ) || pod.metadata.deletion_timestamp.is_some()
}

/// `shouldEnforceResourceVersion` (eviction.go:399-407).
fn should_enforce_resource_version(pod: &Pod) -> bool {
    let phase = pod.status.as_ref().and_then(|s| s.phase.as_ref());
    // We don't need to enforce ResourceVersion for terminal pods.
    if matches!(phase, Some(Phase::Succeeded) | Some(Phase::Failed))
        || pod.metadata.deletion_timestamp.is_some()
    {
        return false;
    }
    // True for all other pods, to ensure we don't race against a pod becoming
    // healthy (ready) and violating PDBs.
    true
}

/// `propagateDryRun` (eviction.go:108-126): the request's dry-run option goes
/// into the eviction's delete options. "It returns an error if they have
/// non-matching dry-run options."
fn propagate_dry_run(
    eviction: &mut rusternetes_common::resources::Eviction,
    options: &rusternetes_common::validation::metav1::CreateOptions,
) -> rusternetes_common::Result<rusternetes_common::deletion::DeleteOptions> {
    use crate::registry::rest::zero_delete_options;
    let request = options.dry_run.clone().filter(|d| !d.is_empty());
    let Some(delete_options) = eviction.delete_options.as_mut() else {
        return Ok(rusternetes_common::deletion::DeleteOptions {
            dry_run: options.dry_run.clone(),
            ..zero_delete_options()
        });
    };
    let own = delete_options.dry_run.clone().filter(|d| !d.is_empty());
    match (own, request) {
        (None, request) => {
            delete_options.dry_run = request.or(options.dry_run.clone());
            Ok(delete_options.clone())
        }
        (Some(_), None) => Ok(delete_options.clone()),
        (Some(own), Some(request)) => {
            if own != request {
                return Err(rusternetes_common::Error::Internal(format!(
                    "Non-matching dry-run options in request and content: {request:?} and {own:?}"
                )));
            }
            Ok(delete_options.clone())
        }
    }
}

/// `&metav1.Status{Status: metav1.StatusSuccess}`: no code, which the create
/// handler turns into a 201 (create.go:227-231).
fn success_status() -> Status {
    Status {
        code: None,
        ..Status::success()
    }
}

/// `createTooManyRequestsError` (eviction.go:413-421).
fn create_too_many_requests_error(name: &str) -> rusternetes_common::Error {
    // TODO upstream: once there are time-based budgets, we can sometimes
    // compute a sensible suggested value. Even without that, a suggestion
    // (even a small one) prevents well-behaved clients from hammering us.
    too_many_requests(
        10,
        vec![rusternetes_common::StatusCause {
            reason: Some("DisruptionBudget".to_string()),
            message: Some(format!(
                "The disruption budget {name} is still being processed by the server."
            )),
            field: None,
        }],
    )
}

/// `errors.NewTooManyRequests(message, retryAfterSeconds)` (errors.go) with
/// the eviction message, and `causes` appended to its `Details`.
fn too_many_requests(
    retry_after_seconds: i32,
    causes: Vec<rusternetes_common::StatusCause>,
) -> rusternetes_common::Error {
    let details = rusternetes_common::StatusDetails {
        name: None,
        group: None,
        kind: None,
        uid: None,
        causes: (!causes.is_empty()).then_some(causes),
        retry_after_seconds: (retry_after_seconds > 0).then_some(retry_after_seconds),
    };
    rusternetes_common::Error::Status(Box::new(Status::failure_with_details(
        "Cannot evict pod as it would violate the pod's disruption budget.",
        "TooManyRequests",
        429,
        details,
    )))
}

/// `errors.NewForbidden(policy.Resource("poddisruptionbudget"), name, err)`.
fn pdb_forbidden(name: &str, err: &str) -> rusternetes_common::Error {
    rusternetes_common::Error::Forbidden(format!(
        "poddisruptionbudget.policy \"{name}\" is forbidden: {err}"
    ))
}

/// The pod storage `EvictionREST` reads and writes through (its `store`,
/// `rest.StandardStorage` upstream — the pod **status** store, see
/// `NewStorage`, storage.go:103-116). A trait so the port of `TestEviction`
/// can script `Delete` with upstream's `mockStore`.
#[async_trait]
pub trait EvictionPodStore: Send + Sync {
    /// `Get`.
    async fn get(&self, ctx: &RequestContext, name: &str) -> rusternetes_common::Result<Pod>;
    /// `Update`, with the validation callbacks of `rest.ValidateAllObjectFunc`.
    async fn update(
        &self,
        ctx: &RequestContext,
        name: &str,
        obj_info: &dyn UpdatedObjectInfo<Pod>,
    ) -> rusternetes_common::Result<Pod>;
    /// `Delete`, with `rest.ValidateAllObjectFunc`.
    async fn delete(
        &self,
        ctx: &RequestContext,
        name: &str,
        options: rusternetes_common::deletion::DeleteOptions,
    ) -> rusternetes_common::Result<()>;
}

#[async_trait]
impl<S: rusternetes_storage::Storage + 'static> EvictionPodStore for Store<Pod, S> {
    async fn get(&self, ctx: &RequestContext, name: &str) -> rusternetes_common::Result<Pod> {
        Store::get(self, ctx, name).await
    }

    async fn update(
        &self,
        ctx: &RequestContext,
        name: &str,
        obj_info: &dyn UpdatedObjectInfo<Pod>,
    ) -> rusternetes_common::Result<Pod> {
        Store::update(
            self,
            ctx,
            name,
            obj_info,
            None,
            None,
            false,
            &crate::registry::generic::UpdateOptions::default(),
        )
        .await
        .map(|(pod, _created)| pod)
    }

    async fn delete(
        &self,
        ctx: &RequestContext,
        name: &str,
        options: rusternetes_common::deletion::DeleteOptions,
    ) -> rusternetes_common::Result<()> {
        Store::delete(self, ctx, name, None, options)
            .await
            .map(|_| ())
    }
}

/// `policyclient.PodDisruptionBudgetsGetter` (the typed client upstream's
/// `EvictionREST` is handed, a loopback client of the api-server itself): the
/// calls `getPodDisruptionBudgets` and `checkAndDecrement` make.
#[async_trait]
pub trait PdbClient: Send + Sync {
    /// `PodDisruptionBudgets(ns).List`.
    async fn list(
        &self,
        namespace: &str,
    ) -> rusternetes_common::Result<Vec<rusternetes_common::resources::PodDisruptionBudget>>;
    /// `PodDisruptionBudgets(ns).Get`.
    async fn get(
        &self,
        namespace: &str,
        name: &str,
    ) -> rusternetes_common::Result<rusternetes_common::resources::PodDisruptionBudget>;
    /// `PodDisruptionBudgets(ns).UpdateStatus`: a write of the whole object
    /// that conflicts when its `resourceVersion` is not the stored one.
    async fn update_status(
        &self,
        namespace: &str,
        pdb: &rusternetes_common::resources::PodDisruptionBudget,
    ) -> rusternetes_common::Result<rusternetes_common::resources::PodDisruptionBudget>;
}

/// The PDB client over the PodDisruptionBudget store (reads) and
/// `Storage::update_status_cas` (the status write).
pub struct StorePdbClient {
    storage: Arc<StorageBackend>,
}

impl StorePdbClient {
    pub fn new(storage: Arc<StorageBackend>) -> Self {
        Self { storage }
    }

    fn ctx(namespace: &str) -> RequestContext {
        RequestContext::new(Some(namespace)).with_group_version("policy", "v1")
    }
}

#[async_trait]
impl PdbClient for StorePdbClient {
    async fn list(
        &self,
        namespace: &str,
    ) -> rusternetes_common::Result<Vec<rusternetes_common::resources::PodDisruptionBudget>> {
        use rusternetes_storage::{build_prefix, Storage};
        let mut pdbs: Vec<rusternetes_common::resources::PodDisruptionBudget> = self
            .storage
            .list(&build_prefix("poddisruptionbudgets", Some(namespace)))
            .await?;
        // The storage codec's defaulting, as `Store::get` applies it.
        for pdb in &mut pdbs {
            crate::registry::policy::poddisruptionbudget::convert_to_internal(pdb);
        }
        Ok(pdbs)
    }

    async fn get(
        &self,
        namespace: &str,
        name: &str,
    ) -> rusternetes_common::Result<rusternetes_common::resources::PodDisruptionBudget> {
        crate::registry::policy::poddisruptionbudget::new_store(self.storage.clone())
            .get(&Self::ctx(namespace), name)
            .await
    }

    async fn update_status(
        &self,
        namespace: &str,
        pdb: &rusternetes_common::resources::PodDisruptionBudget,
    ) -> rusternetes_common::Result<rusternetes_common::resources::PodDisruptionBudget> {
        // The loopback `UpdateStatus` (eviction.go:432, `PodDisruptionBudgets(ns).
        // UpdateStatus`) is a PUT to `/status`: `Store.Update` on the status
        // store (storage.go:60-62), so `podDisruptionBudgetStatusStrategy`
        // runs -- `PrepareForUpdate` keeps the spec (strategy.go:156-161) and
        // `ValidateUpdate` checks the status (:164-174). The object carries
        // its `resourceVersion`, so a PDB changed since it was read is a
        // Conflict, which `RetryOnConflict` re-reads on.
        let info = DefaultUpdatedObjectInfo::new(Some(pdb.clone()), Vec::new());
        let (updated, _) =
            crate::registry::policy::poddisruptionbudget::new_status_store(self.storage.clone())
                .update(
                    &Self::ctx(namespace),
                    &pdb.metadata.name,
                    &info,
                    None,
                    None,
                    false,
                    &UpdateOptions::default(),
                )
                .await?;
        Ok(updated)
    }
}

/// `EvictionREST` (eviction.go:70-74): the `pods/eviction` subresource.
pub struct EvictionRest<P: EvictionPodStore, C: PdbClient> {
    store: P,
    pdb_client: C,
    /// `EvictionsRetry`; a field so a test need not wait out 20 half-second
    /// pauses.
    pub retry: Backoff,
}

/// The `EvictionREST` the router serves.
pub type StoreEvictionRest = EvictionRest<Store<Pod, StorageBackend>, StorePdbClient>;

/// `newEvictionStorage(&statusStore, podDisruptionBudgetClient)`
/// (storage.go:116).
pub fn new_eviction_rest(storage: Arc<StorageBackend>) -> StoreEvictionRest {
    EvictionRest::new(
        new_status_store(storage.clone()),
        StorePdbClient::new(storage),
    )
}

/// `getLatestPod` (eviction.go:319-340): throw away the new object and take
/// the latest pod from storage, so the condition appender cannot conflict;
/// the delete options' preconditions are checked against it.
struct GetLatestPod {
    preconditions: Option<rusternetes_common::deletion::Preconditions>,
}

#[async_trait]
impl TransformFunc<Pod> for GetLatestPod {
    async fn transform(
        &self,
        _ctx: &RequestContext,
        _new: Option<Pod>,
        old: Option<&Pod>,
    ) -> rusternetes_common::Result<Pod> {
        let latest = old.cloned().ok_or_else(|| {
            rusternetes_common::Error::Internal("the pod to evict is not stored".to_string())
        })?;
        let pod_resource = GroupResource::new("", "Pod");
        if let Some(preconditions) = &self.preconditions {
            if let Some(uid) = preconditions.uid.as_deref().filter(|u| !u.is_empty()) {
                if uid != latest.metadata.uid {
                    return Err(crate::registry::rest::conflict(
                        &pod_resource,
                        &latest.metadata.name,
                        format!(
                            "the UID in the precondition ({uid}) does not match the UID in record ({}). The object might have been deleted and then recreated",
                            latest.metadata.uid
                        ),
                    ));
                }
            }
            if let Some(rv) = preconditions
                .resource_version
                .as_deref()
                .filter(|r| !r.is_empty())
            {
                let latest_rv = latest.metadata.resource_version.as_deref().unwrap_or("");
                if rv != latest_rv {
                    return Err(crate::registry::rest::conflict(
                        &pod_resource,
                        &latest.metadata.name,
                        format!(
                            "the ResourceVersion in the precondition ({rv}) does not match the ResourceVersion in record ({latest_rv}). The object might have been modified"
                        ),
                    ));
                }
            }
        }
        Ok(latest)
    }
}

/// `conditionAppender` (eviction.go:342-351): the `DisruptionTarget`
/// condition.
struct ConditionAppender;

#[async_trait]
impl TransformFunc<Pod> for ConditionAppender {
    async fn transform(
        &self,
        _ctx: &RequestContext,
        new: Option<Pod>,
        _old: Option<&Pod>,
    ) -> rusternetes_common::Result<Pod> {
        let mut pod = new.ok_or_else(|| {
            rusternetes_common::Error::Internal("no pod to add the condition to".to_string())
        })?;
        update_pod_condition(
            pod.status.get_or_insert_with(Default::default),
            PodCondition {
                condition_type: "DisruptionTarget".to_string(),
                status: "True".to_string(),
                reason: Some("EvictionByEvictionAPI".to_string()),
                message: Some("Eviction API: evicting".to_string()),
                last_probe_time: None,
                last_transition_time: None,
                observed_generation: None,
            },
        );
        Ok(pod)
    }
}

impl<P: EvictionPodStore, C: PdbClient> EvictionRest<P, C> {
    pub fn new(store: P, pdb_client: C) -> Self {
        Self {
            store,
            pdb_client,
            retry: evictions_retry(),
        }
    }

    /// `EvictionREST.Create` (eviction.go:128-315): "attempts to create a new
    /// eviction. That is, it tries to evict a pod." Returns the `Status` the
    /// handler writes: a success, or the failure `Status` upstream returns as
    /// the *object* (a pod covered by more than one PodDisruptionBudget).
    pub async fn create(
        &self,
        ctx: &RequestContext,
        name: &str,
        mut eviction: rusternetes_common::resources::Eviction,
        create_validation: Option<
            &dyn crate::registry::rest::ValidateObject<rusternetes_common::resources::Eviction>,
        >,
        options: &rusternetes_common::validation::metav1::CreateOptions,
    ) -> rusternetes_common::Result<Status> {
        use rusternetes_common::Error;
        if name != eviction.metadata.name {
            return Err(Error::BadRequest(
                "name in URL does not match name in Eviction object".to_string(),
            ));
        }

        if eviction
            .delete_options
            .as_ref()
            .and_then(|o| o.ignore_store_read_error_with_cluster_breaking_potential)
            .unwrap_or(false)
        {
            return Err(Error::Invalid(vec![FieldError::invalid(
                &Path::new("deleteOptions")
                    .child("ignoreStoreReadErrorWithClusterBreakingPotential"),
                true,
                "can not be set for pod eviction, try after removing the option",
            )]));
        }

        let original_delete_options = propagate_dry_run(&mut eviction, options)?;

        if let Some(validate) = create_validation {
            validate.validate(ctx, &eviction).await?;
        }

        // by default, retry conflict errors; "if the original options included
        // a resourceVersion precondition, don't retry"
        let should_retry: fn(&Error) -> bool =
            if resource_version_is_unset(&original_delete_options) {
                is_conflict
            } else {
                |_| false
            };

        let mut backoff = self.retry.clone();
        let attempt = loop {
            match self
                .delete_if_pdb_can_be_ignored(
                    ctx,
                    &eviction.metadata.name,
                    &original_delete_options,
                )
                .await
            {
                Ok(attempt) => break Ok(attempt),
                Err(err) if should_retry(&err) => {
                    if !backoff.wait().await {
                        // `retry.OnError` hands back the last error once the
                        // attempts are spent.
                        break Err(err);
                    }
                }
                Err(err) => break Err(err),
            }
        };
        let pod = match attempt {
            // this can happen in cases where the PDB can be ignored, but
            // there was a problem issuing the pod delete: maybe we conflicted
            // too many times or we didn't have permission or something else
            // weird.
            Err(err) => return Err(err),
            // we successfully deleted the pod, so we're done: we've
            // evicted/deleted the pod
            Ok((_, true)) => return Ok(success_status()),
            // we cannot ignore the PDB for this pod, so this is the fall
            // through case.
            Ok((pod, false)) => pod,
        };

        let namespace = pod
            .metadata
            .namespace
            .clone()
            .or_else(|| ctx.namespace.clone())
            .unwrap_or_default();
        let mut pdb_name = String::new();
        let mut update_deletion_options = false;

        let pdbs = self.get_pod_disruption_budgets(&namespace, &pod).await?;
        if pdbs.len() > 1 {
            return Ok(Status {
                kind: "Status".to_string(),
                api_version: "v1".to_string(),
                metadata: None,
                status: Some("Failure".to_string()),
                message: Some(
                    "This pod has more than one PodDisruptionBudget, which the eviction subresource does not support."
                        .to_string(),
                ),
                reason: None,
                details: None,
                code: Some(500),
            });
        }
        if let Some(pdb) = pdbs.into_iter().next() {
            pdb_name = pdb.metadata.name.clone();
            let status = pdb.status.clone().unwrap_or_else(default_pdb_status);

            // IsPodReady is the current implementation of IsHealthy. If the
            // pod is healthy, it should be guarded by the PDB.
            let mut check_budget = true;
            if !rusternetes_common::podutil::is_pod_ready(&pod) {
                if pdb.spec.unhealthy_pod_eviction_policy.as_deref() == Some("AlwaysAllow") {
                    // Delete the unhealthy pod, it doesn't count towards
                    // currentHealthy and desiredHealthy and we should not
                    // decrement disruptionsAllowed.
                    update_deletion_options = true;
                    check_budget = false;
                } else if status.current_healthy >= status.desired_healthy
                    && status.desired_healthy > 0
                {
                    // default nil and IfHealthyBudget policy. Delete the
                    // unhealthy pod, it doesn't count towards currentHealthy
                    // and desiredHealthy and we should not decrement
                    // disruptionsAllowed. Application guarded by the PDB is
                    // not disrupted at the moment and deleting unhealthy
                    // (unready) pod will not disrupt it.
                    update_deletion_options = true;
                    check_budget = false;
                }
                // confirm no disruptions allowed in checkAndDecrement
            }

            if check_budget {
                self.decrement_with_retry(
                    &namespace,
                    &pod.metadata.name,
                    pdb,
                    is_dry_run(original_delete_options.dry_run.as_deref()),
                )
                .await?;
            }
        }

        // At this point there was either no PDB or we succeeded in
        // decrementing or the pod was unhealthy (unready) and we have enough
        // healthy replicas.
        let mut delete_options = original_delete_options.clone();

        // Set deleteOptions.Preconditions.ResourceVersion to ensure the pod
        // hasn't been considered healthy (ready) since we calculated.
        if update_deletion_options {
            set_preconditions_resource_version(
                &mut delete_options,
                pod.metadata.resource_version.clone(),
            );
        }

        // Try the delete
        if let Err(err) = self
            .add_condition_and_delete_pod(ctx, &eviction.metadata.name, &delete_options)
            .await
        {
            if is_conflict(&err)
                && update_deletion_options
                && resource_version_is_unset(&original_delete_options)
            {
                // If we encounter a resource conflict error, we updated the
                // deletion options to include them, and the original deletion
                // options did not specify ResourceVersion, we send back
                // TooManyRequests so clients will retry.
                return Err(create_too_many_requests_error(&pdb_name));
            }
            return Err(err);
        }

        // Success!
        Ok(success_status())
    }

    /// One pass of the first `retry.OnError` body (eviction.go:166-197):
    /// fetch the pod and, when its PDB can be ignored, delete it. Returns the
    /// pod and whether it was deleted.
    async fn delete_if_pdb_can_be_ignored(
        &self,
        ctx: &RequestContext,
        name: &str,
        original_delete_options: &rusternetes_common::deletion::DeleteOptions,
    ) -> rusternetes_common::Result<(Pod, bool)> {
        let pod = self.store.get(ctx, name).await?;

        // Evicting a terminal pod should result in direct deletion of pod as
        // it already caused disruption by the time we are evicting. There is
        // no need to check for pdb.
        if !can_ignore_pdb(&pod) {
            // Pod is not in a state where we can skip checking PDBs, exit the
            // loop, and continue to PDB checks.
            return Ok((pod, false));
        }

        // the PDB can be ignored, so delete the pod
        let mut delete_options = original_delete_options.clone();

        // We should check if resourceVersion is already set by the requestor
        // as it might be older than the pod we just fetched and should be
        // honored.
        if should_enforce_resource_version(&pod)
            && resource_version_is_unset(original_delete_options)
        {
            // Set deleteOptions.Preconditions.ResourceVersion to ensure we're
            // not racing with another PDB-impacting process elsewhere.
            set_preconditions_resource_version(
                &mut delete_options,
                pod.metadata.resource_version.clone(),
            );
        }
        self.add_condition_and_delete_pod(ctx, name, &delete_options)
            .await?;
        Ok((pod, true))
    }

    /// The `retry.RetryOnConflict` around `checkAndDecrement`
    /// (eviction.go:256-273): on a conflict the PDB is read again.
    async fn decrement_with_retry(
        &self,
        namespace: &str,
        pod_name: &str,
        pdb: rusternetes_common::resources::PodDisruptionBudget,
        dry_run: bool,
    ) -> rusternetes_common::Result<()> {
        let pdb_name = pdb.metadata.name.clone();
        let mut pdb = pdb;
        let mut refresh = false;
        let mut backoff = self.retry.clone();
        loop {
            let result = async {
                if refresh {
                    pdb = self.pdb_client.get(namespace, &pdb_name).await?;
                }
                // Try to verify-and-decrement.
                //
                // If it was false already, or if it becomes false during the
                // course of our retries, raise an error marked as a 429.
                self.check_and_decrement(namespace, pod_name, pdb.clone(), dry_run)
                    .await
            }
            .await;
            match result {
                Ok(()) => return Ok(()),
                Err(err) => {
                    refresh = true;
                    if !is_conflict(&err) || !backoff.wait().await {
                        return Err(err);
                    }
                }
            }
        }
    }

    /// `addConditionAndDeletePod` (eviction.go:317-372): the pod gets the
    /// `DisruptionTarget` condition, through the status store, and is then
    /// deleted through the pod store, so its graceful-delete strategy runs.
    pub async fn add_condition_and_delete_pod(
        &self,
        ctx: &RequestContext,
        name: &str,
        options: &rusternetes_common::deletion::DeleteOptions,
    ) -> rusternetes_common::Result<()> {
        let mut options = options.clone();
        if !is_dry_run(options.dry_run.as_deref()) {
            // order important
            let pod_updated_object_info = DefaultUpdatedObjectInfo::new(
                None,
                vec![
                    Box::new(GetLatestPod {
                        preconditions: options.preconditions.clone(),
                    }),
                    Box::new(ConditionAppender),
                ],
            );
            let updated = self
                .store
                .update(ctx, name, &pod_updated_object_info)
                .await?;

            if !resource_version_is_unset(&options) {
                // bump the resource version, since we are the one who
                // modified it via the update
                if let Some(preconditions) = options.preconditions.as_mut() {
                    preconditions.resource_version = updated.metadata.resource_version.clone();
                }
            }
        }
        self.store.delete(ctx, name, options).await
    }

    /// `checkAndDecrement` (eviction.go:423-484): "checks if the provided
    /// PodDisruptionBudget allows any disruption", and records the one this
    /// eviction is about to cause.
    async fn check_and_decrement(
        &self,
        namespace: &str,
        pod_name: &str,
        mut pdb: rusternetes_common::resources::PodDisruptionBudget,
        dry_run: bool,
    ) -> rusternetes_common::Result<()> {
        use rusternetes_common::pdbhelper::{
            find_status_condition, update_disruption_allowed_condition,
            DISRUPTION_ALLOWED_CONDITION, SYNC_FAILED_REASON,
        };
        let pdb_name = pdb.metadata.name.clone();
        let generation = pdb.metadata.generation.unwrap_or(0);
        let status = pdb.status.get_or_insert_with(default_pdb_status);
        if status.observed_generation.unwrap_or(0) < generation {
            return Err(create_too_many_requests_error(&pdb_name));
        }
        if status.disruptions_allowed < 0 {
            return Err(pdb_forbidden(
                &pdb_name,
                "pdb disruptions allowed is negative",
            ));
        }
        if status.disrupted_pods.as_ref().map_or(0, |d| d.len()) > MAX_DISRUPTED_POD_SIZE {
            return Err(pdb_forbidden(
                &pdb_name,
                "DisruptedPods map too big - too many evictions not confirmed by PDB controller",
            ));
        }
        if status.disruptions_allowed == 0 {
            let condition = status
                .conditions
                .as_deref()
                .and_then(|c| find_status_condition(c, DISRUPTION_ALLOWED_CONDITION));
            let failed = condition.filter(|c| {
                c.status == "False" && c.message.as_deref().is_some_and(|m| !m.is_empty())
            });
            let msg = match failed {
                // check whether sync is failed first because DesiredHealthy
                // and CurrentHealthy are not trustworthy when the sync is
                // failed
                Some(c) if c.reason.as_deref() == Some(SYNC_FAILED_REASON) => format!(
                    "The disruption budget {pdb_name} does not allow evicting pods currently because it failed sync: {}",
                    c.message.as_deref().unwrap_or("")
                ),
                _ if status.current_healthy <= status.desired_healthy => format!(
                    "The disruption budget {pdb_name} needs {} healthy pods and has {} currently",
                    status.desired_healthy, status.current_healthy
                ),
                Some(c) => format!(
                    "The disruption budget {pdb_name} does not allow evicting pods currently ({}): {}",
                    c.reason.as_deref().unwrap_or(""),
                    c.message.as_deref().unwrap_or("")
                ),
                None => format!(
                    "The disruption budget {pdb_name} does not allow evicting pods currently"
                ),
            };
            return Err(too_many_requests(
                0,
                vec![rusternetes_common::StatusCause {
                    reason: Some("DisruptionBudget".to_string()),
                    message: Some(msg),
                    field: None,
                }],
            ));
        }

        status.disruptions_allowed -= 1;
        let now_zero = status.disruptions_allowed == 0;
        if now_zero {
            update_disruption_allowed_condition(&mut pdb);
        }

        // If this is a dry-run, we don't need to go any further than that.
        if dry_run {
            return Ok(());
        }

        // Eviction handler needs to inform the PDB controller that it is
        // about to delete a pod so it should not consider it as available in
        // calculations when updating PodDisruptions allowed. If the pod is
        // not deleted within a reasonable time limit PDB controller will
        // assume that it won't be deleted at all and remove it from
        // DisruptedPod map.
        pdb.status
            .get_or_insert_with(default_pdb_status)
            .disrupted_pods
            .get_or_insert_with(Default::default)
            .insert(pod_name.to_string(), chrono::Utc::now());
        self.pdb_client.update_status(namespace, &pdb).await?;
        Ok(())
    }

    /// `getPodDisruptionBudgets` (eviction.go:486-511): "any PDBs that match
    /// the pod".
    async fn get_pod_disruption_budgets(
        &self,
        namespace: &str,
        pod: &Pod,
    ) -> rusternetes_common::Result<Vec<rusternetes_common::resources::PodDisruptionBudget>> {
        let pdbs = self.pdb_client.list(namespace).await?;
        let pod_labels = pod.metadata.labels.clone().unwrap_or_default();
        Ok(pdbs
            .into_iter()
            .filter(|pdb| pdb.metadata.namespace.as_deref().unwrap_or(namespace) == namespace)
            // `LabelSelectorAsSelector`: a nil selector matches nothing, an
            // empty one everything, and an invalid one (an error) skips the
            // PDB: "it does not match the pod".
            .filter(|pdb| {
                pdb.spec
                    .selector
                    .as_ref()
                    .is_some_and(|s| s.matches_labels(&pod_labels))
            })
            .collect())
    }
}

/// The zero `policyv1.PodDisruptionBudgetStatus`.
fn default_pdb_status() -> rusternetes_common::resources::PodDisruptionBudgetStatus {
    rusternetes_common::resources::PodDisruptionBudgetStatus {
        current_healthy: 0,
        desired_healthy: 0,
        disruptions_allowed: 0,
        expected_pods: 0,
        observed_generation: None,
        conditions: None,
        disrupted_pods: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
}
