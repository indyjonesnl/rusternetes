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

use rusternetes_common::equality::semantic_equal;
use rusternetes_common::resources::pod::PodSpec;
use rusternetes_common::resources::{Binding, Container, Pod, PodCondition, PodStatus};
use rusternetes_common::types::Phase;
use rusternetes_common::validation::field::{Error as FieldError, ErrorList, Path};
use rusternetes_common::validation::metav1::{get_warnings_for_ip, is_dns1123_label};
use rusternetes_common::validation::objectmeta::{
    name_is_dns_subdomain, validate_object_meta, validate_object_meta_update,
};
use rusternetes_storage::StorageBackend;

use crate::registry::generic::Store;
use crate::registry::rest::{
    reset_object_meta_for_status, GroupResource, NamespaceScopedStrategy, RequestContext,
    RestCreateStrategy, RestDeleteStrategy, RestGracefulDeleteStrategy, RestUpdateStrategy,
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
            return Vec::new();
        }
        vec![format!(
            "metadata.name: this is used in the Pod's hostname, which can result in surprising behavior; a DNS label is recommended: [{}]",
            msgs.join(" ")
        )]
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

/// `podutil.UpdatePodCondition` (pkg/api/v1/pod/util.go): replace the
/// condition of the same type, or append it. The transition time moves only
/// when the status flips.
fn update_pod_condition(status: &mut PodStatus, mut condition: PodCondition) {
    condition.last_transition_time = Some(chrono::Utc::now());
    let conditions = status.conditions.get_or_insert_with(Vec::new);
    match conditions
        .iter_mut()
        .find(|c| c.condition_type == condition.condition_type)
    {
        None => conditions.push(condition),
        Some(old) => {
            if condition.status == old.status {
                condition.last_transition_time = old.last_transition_time;
            }
            *old = condition;
        }
    }
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
