pub mod certificates;
pub mod pod_security_api;
pub mod pod_security_policy;
pub mod resourcequota;
pub mod storage_object_in_use_protection;

/// Pod admission controllers for ResourceQuota, LimitRange enforcement, and ServiceAccount injection
use rusternetes_common::{
    quantity::{parse_resource_value, Quantity},
    resources::{LimitRange, Pod, ServiceAccount},
    types::ResourceRequirements,
};
use rusternetes_storage::Storage;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use tracing::{info, warn};

/// One lock per namespace, serialising pod quota admission within it.
static QUOTA_NAMESPACE_LOCKS: std::sync::LazyLock<
    std::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
> = std::sync::LazyLock::new(Default::default);

/// Serialise quota admission in `namespace`: hold the returned guard across
/// the check and the write of the quota's `status.used`.
///
/// Upstream never evaluates two requests of one namespace at once.
/// `quotaEvaluator.addWork` queues each request under its namespace and
/// `getWork` marks the namespace `inProgress`, parking later arrivals in
/// `dirtyWork` until `completeWork` (`staging/src/k8s.io/apiserver/pkg/
/// admission/plugin/resourcequota/controller.go:688-735`); each admitted
/// request's usage is written to `status.used` before the next is checked
/// (`checkQuotas`, :228-401). [`resourcequota::evaluate`] takes this lock for
/// that span.
///
/// Single api-server only: upstream's cross-apiserver safety is the optimistic
/// `UpdateStatus` on the quota, which `resourcequota::check_quotas` also does.
pub async fn lock_namespace_quota(namespace: &str) -> tokio::sync::OwnedMutexGuard<()> {
    let lock = {
        let mut locks = QUOTA_NAMESPACE_LOCKS
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // Drop namespaces nobody is admitting in, so the map stays bounded by
        // the number of namespaces with an admission in flight.
        locks.retain(|ns, l| ns == namespace || Arc::strong_count(l) > 1);
        locks.entry(namespace.to_string()).or_default().clone()
    };
    lock.lock_owned().await
}

/// Apply LimitRange defaults and validate constraints
#[allow(dead_code)]
pub async fn apply_limit_range<S: Storage>(
    storage: &Arc<S>,
    namespace: &str,
    pod: &mut Pod,
) -> anyhow::Result<bool> {
    let limit_prefix = format!("/registry/limitranges/{}/", namespace);
    let limit_ranges: Vec<LimitRange> = storage.list(&limit_prefix).await?;
    apply_limit_range_with(pod, &limit_ranges)
}

/// Apply LimitRange defaults and validate constraints using pre-fetched LimitRanges.
/// Use this when the caller already has the LimitRange list to avoid a redundant storage read.
pub fn apply_limit_range_with(
    pod: &mut Pod,
    limit_ranges: &Vec<LimitRange>,
) -> anyhow::Result<bool> {
    if limit_ranges.is_empty() {
        // No limits to apply
        return Ok(true);
    }

    // Apply defaults and validate for each container
    if let Some(spec) = &mut pod.spec {
        for container in &mut spec.containers {
            for limit_range in limit_ranges {
                for limit_item in &limit_range.spec.limits {
                    // Only apply Container limits to containers
                    if limit_item.item_type == "Container" {
                        // Apply defaults if not specified
                        if container.resources.is_none() {
                            container.resources = Some(ResourceRequirements {
                                limits: None,
                                requests: None,
                                claims: None,
                            });
                        }

                        let resources = container.resources.as_mut().unwrap();

                        // Apply default limits
                        if let Some(default_limits) = &limit_item.default {
                            if let Some(limits) = resources.limits.as_mut() {
                                // Merge with existing limits
                                for (key, value) in default_limits {
                                    limits.entry(key.clone()).or_insert_with(|| value.clone());
                                }
                            } else {
                                resources.limits = Some(default_limits.clone());
                            }
                        }

                        // Apply defaultRequest for missing request resources.
                        // If defaultRequest is not defined, fall back to default (limits).
                        let effective_defaults = limit_item
                            .default_request
                            .as_ref()
                            .or(limit_item.default.as_ref());
                        if let Some(defaults) = effective_defaults {
                            let requests = resources.requests.get_or_insert_with(HashMap::new);
                            for (key, value) in defaults {
                                requests.entry(key.clone()).or_insert_with(|| value.clone());
                            }
                        }

                        // Validate min constraints
                        if let Some(min) = &limit_item.min {
                            if !validate_min_resources(resources, min, &container.name)? {
                                return Ok(false);
                            }
                        }

                        // Validate max constraints
                        if let Some(max) = &limit_item.max {
                            if !validate_max_resources(resources, max, &container.name)? {
                                return Ok(false);
                            }
                        }

                        // Validate max limit/request ratio
                        if let Some(ratio) = &limit_item.max_limit_request_ratio {
                            if !validate_ratio(resources, ratio, &container.name)? {
                                return Ok(false);
                            }
                        }
                    }
                }
            }
        }
    }

    // Pod-level aggregation: `type: Pod` items bound the SUM of a resource
    // across ALL containers in the pod, not each container individually.
    // Upstream: `PodValidateLimitFunc` in
    // `plugin/pkg/admission/limitranger/admission.go` sums per-resource over
    // containers and checks the total against min/max.
    if let Some(spec) = &pod.spec {
        // Sum a resource (in canonical units — cpu millicores, else bytes)
        // across every container's `requests` (or `limits` when `use_limits`).
        let sum_across = |use_limits: bool, resource: &str| -> anyhow::Result<i64> {
            let mut total = 0i64;
            for container in &spec.containers {
                if let Some(rr) = &container.resources {
                    let map = if use_limits { &rr.limits } else { &rr.requests };
                    if let Some(m) = map {
                        if let Some(v) = m.get(resource) {
                            total += if resource == "cpu" {
                                parse_cpu_to_millicores(v)?
                            } else {
                                parse_memory_to_bytes(v)?
                            };
                        }
                    }
                }
            }
            Ok(total)
        };

        for limit_range in limit_ranges {
            for limit_item in &limit_range.spec.limits {
                if limit_item.item_type != "Pod" {
                    continue;
                }

                // max: summed requests AND summed limits must each be ≤ max.
                if let Some(max) = &limit_item.max {
                    for (resource, max_value) in max {
                        for use_limits in [false, true] {
                            let sum = sum_across(use_limits, resource)?;
                            let exceeds = if resource == "cpu" {
                                sum > parse_cpu_to_millicores(max_value)?
                            } else {
                                sum > parse_memory_to_bytes(max_value)?
                            };
                            if exceeds {
                                warn!(
                                    "Pod {} aggregate {} {} exceeds type:Pod maximum {}",
                                    pod.metadata.name,
                                    if use_limits { "limits" } else { "requests" },
                                    resource,
                                    max_value
                                );
                                return Ok(false);
                            }
                        }
                    }
                }

                // min: summed requests must be ≥ min.
                if let Some(min) = &limit_item.min {
                    for (resource, min_value) in min {
                        let sum = sum_across(false, resource)?;
                        let below = if resource == "cpu" {
                            sum < parse_cpu_to_millicores(min_value)?
                        } else {
                            sum < parse_memory_to_bytes(min_value)?
                        };
                        if below {
                            warn!(
                                "Pod {} aggregate requests {} below type:Pod minimum {}",
                                pod.metadata.name, resource, min_value
                            );
                            return Ok(false);
                        }
                    }
                }
            }
        }
    }

    Ok(true)
}

/// `requestLimitEnforcedValues` (limitranger/admission.go:295-306): compare in
/// milli-units.
fn exceeds(observed: &Quantity, enforced: &Quantity) -> std::cmp::Ordering {
    observed.milli_value().cmp(&enforced.milli_value())
}

/// Port of `PersistentVolumeClaimValidateLimitFunc`
/// (plugin/pkg/admission/limitranger/admission.go:451-473): every
/// `type: PersistentVolumeClaim` item's `min` (`minConstraint`, :309-324, with
/// no limits) and `max` (`maxRequestConstraint`, :328-339) against the
/// claim's requests. Returns the errors `utilerrors.NewAggregate` would
/// aggregate. Keys are walked sorted where Go's map order is random.
pub fn persistent_volume_claim_validate_limit(
    limit_range: &LimitRange,
    pvc: &rusternetes_common::resources::PersistentVolumeClaim,
) -> Vec<String> {
    let empty = HashMap::new();
    let requests = pvc.spec.resources.requests.as_ref().unwrap_or(&empty);
    // Strategy validation has already rejected an unparseable quantity.
    let request = |name: &str| requests.get(name).and_then(|v| Quantity::parse(v).ok());
    let sorted = |m: &Option<HashMap<String, String>>| {
        let mut entries: Vec<(String, String)> = m
            .iter()
            .flatten()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        entries.sort();
        entries
    };
    let mut errs = Vec::new();
    for limit in &limit_range.spec.limits {
        let limit_type = limit.item_type.as_str();
        if limit_type != "PersistentVolumeClaim" {
            continue;
        }
        for (k, v) in sorted(&limit.min) {
            let Ok(enforced) = Quantity::parse(&v) else {
                continue;
            };
            match request(&k) {
                None => errs.push(format!(
                    "minimum {k} usage per {limit_type} is {enforced}.  No request is specified"
                )),
                Some(req) if exceeds(&req, &enforced).is_lt() => errs.push(format!(
                    "minimum {k} usage per {limit_type} is {enforced}, but request is {req}"
                )),
                _ => {}
            }
        }
        for (k, v) in sorted(&limit.max) {
            let Ok(enforced) = Quantity::parse(&v) else {
                continue;
            };
            match request(&k) {
                None => errs.push(format!(
                    "maximum {k} usage per {limit_type} is {enforced}.  No request is specified"
                )),
                Some(req) if exceeds(&req, &enforced).is_gt() => errs.push(format!(
                    "maximum {k} usage per {limit_type} is {enforced}, but request is {req}"
                )),
                _ => {}
            }
        }
    }
    errs
}

/// `utilerrors.NewAggregate(errs).Error()`
/// (apimachinery/pkg/util/errors/errors.go): one error alone, several in
/// brackets.
fn aggregate(errs: &[String]) -> String {
    match errs {
        [one] => one.clone(),
        many => format!("[{}]", many.join(", ")),
    }
}

/// `LimitRanger.Validate` for a PersistentVolumeClaim
/// (limitranger/admission.go:116-156): each LimitRange in the namespace, the
/// first to fail rejecting the claim. Returns that failure's message.
pub async fn limit_ranger_validate_pvc<S: Storage>(
    storage: &Arc<S>,
    namespace: &str,
    pvc: &rusternetes_common::resources::PersistentVolumeClaim,
) -> anyhow::Result<Option<String>> {
    let limit_prefix = format!("/registry/limitranges/{}/", namespace);
    let limit_ranges: Vec<LimitRange> = storage.list(&limit_prefix).await?;
    for limit_range in &limit_ranges {
        let errs = persistent_volume_claim_validate_limit(limit_range, pvc);
        if !errs.is_empty() {
            return Ok(Some(aggregate(&errs)));
        }
    }
    Ok(None)
}

fn validate_min_resources(
    resources: &ResourceRequirements,
    min: &HashMap<String, String>,
    container_name: &str,
) -> anyhow::Result<bool> {
    // Check requests against min
    if let Some(requests) = &resources.requests {
        for (resource, min_value) in min {
            if let Some(request_value) = requests.get(resource) {
                let below = if resource == "cpu" {
                    parse_cpu_to_millicores(request_value)? < parse_cpu_to_millicores(min_value)?
                } else {
                    parse_memory_to_bytes(request_value)? < parse_memory_to_bytes(min_value)?
                };
                if below {
                    warn!(
                        "Container {} has {} request {} below minimum {}",
                        container_name, resource, request_value, min_value
                    );
                    return Ok(false);
                }
            }
        }
    }
    // Check limits against min — K8s enforces min on both
    if let Some(limits) = &resources.limits {
        for (resource, min_value) in min {
            if let Some(limit_value) = limits.get(resource) {
                let below = if resource == "cpu" {
                    parse_cpu_to_millicores(limit_value)? < parse_cpu_to_millicores(min_value)?
                } else {
                    parse_memory_to_bytes(limit_value)? < parse_memory_to_bytes(min_value)?
                };
                if below {
                    warn!(
                        "Container {} has {} limit {} below minimum {}",
                        container_name, resource, limit_value, min_value
                    );
                    return Ok(false);
                }
            }
        }
    }

    Ok(true)
}

fn validate_max_resources(
    resources: &ResourceRequirements,
    max: &HashMap<String, String>,
    container_name: &str,
) -> anyhow::Result<bool> {
    // Check limits against max
    if let Some(limits) = &resources.limits {
        for (resource, max_value) in max {
            if let Some(limit_value) = limits.get(resource) {
                let exceeds = compare_resource_values(resource, limit_value, max_value)?;
                if exceeds {
                    warn!(
                        "Container {} has {} limit {} exceeding maximum {}",
                        container_name, resource, limit_value, max_value
                    );
                    return Ok(false);
                }
            }
        }
    }
    // Check requests against max — K8s enforces max on both limits and requests
    if let Some(requests) = &resources.requests {
        for (resource, max_value) in max {
            if let Some(request_value) = requests.get(resource) {
                let exceeds = compare_resource_values(resource, request_value, max_value)?;
                if exceeds {
                    warn!(
                        "Container {} has {} request {} exceeding maximum {}",
                        container_name, resource, request_value, max_value
                    );
                    return Ok(false);
                }
            }
        }
    }

    Ok(true)
}

/// Compare a resource value against a limit, returns true if value > limit.
/// Handles cpu, memory, ephemeral-storage, and other resources.
fn compare_resource_values(resource: &str, value: &str, limit: &str) -> anyhow::Result<bool> {
    if resource == "cpu" {
        Ok(parse_cpu_to_millicores(value)? > parse_cpu_to_millicores(limit)?)
    } else {
        // memory, ephemeral-storage, and other byte-based resources
        Ok(parse_memory_to_bytes(value)? > parse_memory_to_bytes(limit)?)
    }
}

fn validate_ratio(
    resources: &ResourceRequirements,
    max_ratio: &HashMap<String, String>,
    container_name: &str,
) -> anyhow::Result<bool> {
    if let (Some(limits), Some(requests)) = (&resources.limits, &resources.requests) {
        for (resource, max_ratio_str) in max_ratio {
            if let (Some(limit_value), Some(request_value)) =
                (limits.get(resource), requests.get(resource))
            {
                let ratio_limit = max_ratio_str.parse::<f64>()?;

                if resource == "cpu" {
                    let limit = parse_cpu_to_millicores(limit_value)? as f64;
                    let request = parse_cpu_to_millicores(request_value)? as f64;
                    if request > 0.0 {
                        let actual_ratio = limit / request;
                        if actual_ratio > ratio_limit {
                            warn!(
                                "Container {} has CPU limit/request ratio {:.2} exceeding maximum {:.2}",
                                container_name, actual_ratio, ratio_limit
                            );
                            return Ok(false);
                        }
                    }
                } else if resource == "memory" {
                    let limit = parse_memory_to_bytes(limit_value)? as f64;
                    let request = parse_memory_to_bytes(request_value)? as f64;
                    if request > 0.0 {
                        let actual_ratio = limit / request;
                        if actual_ratio > ratio_limit {
                            warn!(
                                "Container {} has memory limit/request ratio {:.2} exceeding maximum {:.2}",
                                container_name, actual_ratio, ratio_limit
                            );
                            return Ok(false);
                        }
                    }
                }
            }
        }
    }

    Ok(true)
}

/// Parse a CPU quantity into millicores.
///
/// Upstream never does this: `ResourceQuota.spec.hard`, `LimitRange` bounds and
/// container resources are all typed `resource.Quantity` in Go, parsed once at
/// decode time and compared with `Quantity.Cmp`. Rusternetes carries them as
/// `String`, so every comparison re-parses — hence one shared implementation
/// rather than a suffix chain per call site.
///
/// Millicores/bytes are the units upstream's scheduler accounts these in
/// (`Resource.Add`, `../kubernetes/pkg/scheduler/framework/types.go:917-918`),
/// and `Quantity` rounds both up away from zero as upstream `ScaledValue` does.
fn parse_cpu_to_millicores(cpu: &str) -> anyhow::Result<i64> {
    Ok(parse_resource_value(cpu, "cpu")?)
}

/// Parse a byte-denominated quantity (memory, ephemeral-storage, PVC storage)
/// into bytes. See [`parse_cpu_to_millicores`] for why this is shared.
///
/// The `trim_end_matches` chain this replaced handled `Ti`/`Pi`/`Ei`/`T`/`P`/`E`
/// nowhere, matched only an uppercase `K` — so the non-upstream `"1K"` parsed
/// while the valid `"1k"` did not — and stripped *repeated* suffixes, so
/// `"1GiGi"` read as 1Gi.
fn parse_memory_to_bytes(memory: &str) -> anyhow::Result<i64> {
    Ok(parse_resource_value(memory, "memory")?)
}

/// `DefaultStorageClass` admission (plugin/pkg/admission/storage/storageclass/
/// setdefault/admission.go, `Admit`): a claim that asks for no class
/// (`helper.PersistentVolumeClaimHasClass`: neither `storageClassName` nor the
/// beta annotation) gets the default class, if there is one.
pub async fn set_default_storage_class<S: Storage>(
    storage: &Arc<S>,
    pvc: &mut rusternetes_common::resources::PersistentVolumeClaim,
) -> anyhow::Result<()> {
    let has_beta_annotation = pvc
        .metadata
        .annotations
        .as_ref()
        .is_some_and(|a| a.contains_key("volume.beta.kubernetes.io/storage-class"));
    if pvc.spec.storage_class_name.is_some() || has_beta_annotation {
        return Ok(());
    }
    let storage_classes: Vec<rusternetes_common::resources::StorageClass> =
        storage.list("/registry/storageclasses/").await?;
    if let Some(default) = get_default_class(storage_classes) {
        info!(
            "Setting default storage class '{}' for PVC {}/{}",
            default.metadata.name,
            pvc.metadata.namespace.as_deref().unwrap_or_default(),
            pvc.metadata.name
        );
        pvc.spec.storage_class_name = Some(default.metadata.name);
    }
    Ok(())
}

/// `GetDefaultClass` (pkg/volume/util/storageclass.go:40-71): of the classes
/// annotated default (`IsDefaultAnnotation`, :76-85), the newest, then the
/// first by name.
fn get_default_class(
    classes: Vec<rusternetes_common::resources::StorageClass>,
) -> Option<rusternetes_common::resources::StorageClass> {
    // Shared with the PV controller.
    rusternetes_common::resources::volume::get_default_class(classes)
}

/// ServiceAccount admission controller - injects service account token volumes into pods
/// This is a built-in admission controller that:
/// 1. Sets serviceAccountName to "default" if not specified
/// 2. Injects a volume for the service account token secret
/// 3. Mounts the token at /var/run/secrets/kubernetes.io/serviceaccount/ in all containers
pub async fn inject_service_account_token<S: Storage>(
    storage: &Arc<S>,
    namespace: &str,
    pod: &mut Pod,
) -> anyhow::Result<()> {
    let spec = match &mut pod.spec {
        Some(spec) => spec,
        None => return Ok(()), // No spec, nothing to inject
    };

    // Set service account name to "default" if not specified
    let sa_name = spec
        .service_account_name
        .clone()
        .unwrap_or_else(|| "default".to_string());

    if spec.service_account_name.is_none() {
        info!(
            "Setting default service account for pod {}/{}",
            namespace, pod.metadata.name
        );
        spec.service_account_name = Some(sa_name.clone());
    }

    // Look up the ServiceAccount once: we need both its automount setting and
    // its imagePullSecrets list below.
    let sa_key = format!("/registry/serviceaccounts/{}/{}", namespace, sa_name);
    let service_account = match storage.get::<ServiceAccount>(&sa_key).await {
        Ok(sa) => Some(sa),
        Err(_) => {
            warn!(
                "Service account {}/{} does not exist, but proceeding with token injection",
                namespace, sa_name
            );
            None
        }
    };
    let sa_automount = service_account
        .as_ref()
        .and_then(|sa| sa.automount_service_account_token);

    // Propagate the ServiceAccount's imagePullSecrets onto the pod (semantics
    // documented on the shared helper: pod list wins, regardless of automount).
    let copied = rusternetes_common::serviceaccount::propagate_image_pull_secrets(
        spec,
        service_account
            .as_ref()
            .and_then(|sa| sa.image_pull_secrets.as_deref()),
    );
    if copied > 0 {
        info!(
            "Propagated {} imagePullSecret(s) from SA {}/{} to pod {}",
            copied, namespace, sa_name, pod.metadata.name
        );
    }

    // Determine whether to mount the SA token.
    // Pod-level setting takes precedence over SA-level.
    let pod_automount = spec.automount_service_account_token;
    let should_mount = match pod_automount {
        Some(false) => false,                 // Pod explicitly disabled
        Some(true) => true,                   // Pod explicitly enabled
        None => sa_automount.unwrap_or(true), // Use SA setting, default true
    };

    if !should_mount {
        info!(
            "Skipping service account token injection for pod {}/{} - automountServiceAccountToken is false",
            namespace, pod.metadata.name
        );
        return Ok(());
    }

    // Inject the projected kube-api-access volume (token + ca.crt + namespace)
    // and mount it into every container. Shared with the controller-manager so
    // controller-created pods (ReplicaSet/StatefulSet/etc.) get the same volume
    // — they write pods straight to storage and bypass this HTTP admission path.
    rusternetes_common::serviceaccount::add_kube_api_access_volume(spec);

    info!(
        "Service account token injection complete for pod {}/{} using SA {}",
        namespace, pod.metadata.name, sa_name
    );

    Ok(())
}

/// `ignoredPodSubresources` (pod-security-admission/admission/admission.go:316-325).
/// Any other subresource is expected to be a Pod and is evaluated.
const IGNORED_POD_SUBRESOURCES: [&str; 8] = [
    "exec",
    "attach",
    "binding",
    "eviction",
    "log",
    "portforward",
    "proxy",
    "status",
];

/// `isSignificantPodUpdate` (pod-security-admission/admission/admission.go:632-666):
/// a pod update triggers policy evaluation only if a container or init
/// container was added or removed, an image changed, or an ephemeral
/// container was added or its image changed. Relevant mutable pod fields
/// are the image fields (`isSignificantContainerUpdate`, :669-671).
pub fn is_significant_pod_update(pod: &Pod, old_pod: &Pod) -> bool {
    use rusternetes_common::resources::pod::Container;
    let empty = Vec::new();
    let (spec, old_spec) = match (&pod.spec, &old_pod.spec) {
        (Some(s), Some(o)) => (s, o),
        // No spec on either side: nothing to evaluate against.
        (None, None) => return false,
        _ => return true,
    };
    let init = spec.init_containers.as_ref().unwrap_or(&empty);
    let old_init = old_spec.init_containers.as_ref().unwrap_or(&empty);
    if spec.containers.len() != old_spec.containers.len() || init.len() != old_init.len() {
        return true;
    }
    let image_changed =
        |new: &[Container], old: &[Container]| new.iter().zip(old).any(|(c, o)| c.image != o.image);
    if image_changed(&spec.containers, &old_spec.containers) || image_changed(init, old_init) {
        return true;
    }
    let old_eph = old_spec.ephemeral_containers.as_deref().unwrap_or(&[]);
    spec.ephemeral_containers.iter().flatten().any(|c| {
        match old_eph.iter().find(|o| o.name == c.name) {
            None => true, // EphemeralContainer added
            Some(o) => c.image != o.image,
        }
    })
}

/// PodSecurityAdmission — stub for the Kubernetes Pod Security Admission
/// (PSA) plugin.
///
/// PSA replaced the now-removed PodSecurityPolicy (PSP) in v1.25. Each
/// namespace selects a Pod Security Standard via the
/// `pod-security.kubernetes.io/enforce` label (`privileged`, `baseline`, or
/// `restricted`) and the admission plugin rejects pods that violate the
/// standard.
///
/// This struct exists so the api-server can wire a single PSA admission
/// plugin into the pod create / update flow. The current `admit()`
/// implementation is intentionally an **allow-all** stub: a small surface
/// area we can grow into a full enforcer without touching every callsite.
///
/// Today, partial PSA enforcement (privileged, hostPID / hostNetwork /
/// hostIPC) lives inline in `handlers::pod::create_pod`. The longer-term
/// plan is to fold that logic — plus volume types, runAsUser, capabilities,
/// seccomp / AppArmor profiles, etc. — into [`PodSecurityAdmission::admit`].
///
/// Upstream references:
/// - <https://kubernetes.io/docs/concepts/security/pod-security-admission/>
/// - <https://kubernetes.io/docs/concepts/security/pod-security-standards/>
/// - `staging/src/k8s.io/pod-security-admission/admission/admission.go`
#[derive(Debug, Default, Clone)]
pub struct PodSecurityAdmission {
    exemptions: PodSecurityExemptions,
}

/// `PodSecurityExemptions`
/// (staging/src/k8s.io/pod-security-admission/admission/api/types.go:40-44),
/// the `exemptions` of the PodSecurity plugin configuration.
#[derive(Debug, Default, Clone, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct PodSecurityExemptions {
    pub usernames: Vec<String>,
    pub namespaces: Vec<String>,
    pub runtime_classes: Vec<String>,
}

impl PodSecurityExemptions {
    /// `Admission.exemptNamespace` (admission.go:673-679): an empty name
    /// is never exempt.
    pub fn exempt_namespace(&self, namespace: &str) -> bool {
        !namespace.is_empty() && self.namespaces.iter().any(|n| n == namespace)
    }

    /// `Admission.exemptUser` (admission.go:680-686).
    pub fn exempt_user(&self, username: &str) -> bool {
        !username.is_empty() && self.usernames.iter().any(|u| u == username)
    }

    /// `Admission.exemptRuntimeClass` (admission.go:687-693): a nil or
    /// empty class is never exempt.
    pub fn exempt_runtime_class(&self, runtime_class: Option<&str>) -> bool {
        runtime_class
            .is_some_and(|rc| !rc.is_empty() && self.runtime_classes.iter().any(|r| r == rc))
    }

    /// The PodSecurity plugin's `exemptions` out of an
    /// `AdmissionConfiguration` document
    /// (`--admission-control-config-file`): the entry named `PodSecurity`,
    /// its inline `configuration` (a `PodSecurityConfiguration`). No such
    /// entry means no exemptions (the plugin's defaults,
    /// admission/api/v1/defaults.go).
    pub fn from_admission_configuration(yaml: &str) -> Result<Self, String> {
        let doc: serde_json::Value = serde_yaml::from_str(yaml).map_err(|e| e.to_string())?;
        let Some(cfg) = doc
            .get("plugins")
            .and_then(|p| p.as_array())
            .into_iter()
            .flatten()
            .find(|p| p.get("name").and_then(|n| n.as_str()) == Some("PodSecurity"))
            .and_then(|p| p.get("configuration"))
        else {
            return Ok(Self::default());
        };
        match cfg.get("exemptions") {
            Some(e) => serde_json::from_value(e.clone()).map_err(|e| e.to_string()),
            None => Ok(Self::default()),
        }
    }
}

static CONFIGURED_EXEMPTIONS: std::sync::OnceLock<PodSecurityExemptions> =
    std::sync::OnceLock::new();

/// Install the process-wide PodSecurity exemptions, read once at startup
/// from the admission control config file.
pub fn install_pod_security_exemptions(exemptions: PodSecurityExemptions) {
    let _ = CONFIGURED_EXEMPTIONS.set(exemptions);
}

impl PodSecurityAdmission {
    /// Create a new PSA admission plugin instance using the exemptions
    /// installed at startup (none by default).
    pub fn new() -> Self {
        Self {
            exemptions: CONFIGURED_EXEMPTIONS.get().cloned().unwrap_or_default(),
        }
    }

    /// A plugin instance with explicit exemptions.
    #[allow(dead_code)]
    pub fn with_exemptions(exemptions: PodSecurityExemptions) -> Self {
        Self { exemptions }
    }

    /// Whether a pod CREATE/UPDATE reaches policy evaluation at all: the
    /// gates of `Admission.ValidatePod`
    /// (staging/src/k8s.io/pod-security-admission/admission/admission.go:329-389).
    ///
    /// A request on an ignored subresource (`ignoredPodSubresources`, :316)
    /// is allowed; an UPDATE is evaluated only when
    /// [`is_significant_pod_update`] says so (:383-386). Every other
    /// subresource (`ephemeralcontainers`, `resize`, ...) is evaluated.
    pub fn should_evaluate(subresource: Option<&str>, old: Option<&Pod>, pod: &Pod) -> bool {
        if subresource.is_some_and(|s| IGNORED_POD_SUBRESOURCES.contains(&s)) {
            return false;
        }
        old.is_none_or(|old| is_significant_pod_update(pod, old))
    }

    /// Evaluate a pod against the namespace's enforced Pod Security
    /// Standard.
    ///
    /// Returns `Ok(())` to admit the pod, `Err(Forbidden)` to reject.
    ///
    /// Enforcement keys off the namespace's
    /// `pod-security.kubernetes.io/enforce` label (and its `-version`).
    /// An absent label or `privileged` admits everything; `baseline` and
    /// `restricted` evaluate the versioned check registry.
    ///
    /// Upstream parity:
    /// `staging/src/k8s.io/pod-security-admission/policy/` (release-1.35).
    /// `username` is the requester, so a user exemption can apply.
    #[allow(dead_code)]
    pub async fn admit_as<S: Storage>(
        &self,
        storage: &Arc<S>,
        namespace: &str,
        pod: &Pod,
        username: &str,
    ) -> Result<(), rusternetes_common::Error> {
        self.admit_outcome(storage, namespace, pod, username)
            .await
            .map(|_| ())
    }

    /// `Admission.ValidatePod` + `EvaluatePod` for an admitted-or-denied
    /// pod (admission.go:329-389, :455-528): the enforce / audit / warn
    /// policy comes from the namespace labels (`PolicyToEvaluate`). Enforce
    /// denies; audit adds the `audit-violations` annotation; warn adds a
    /// warning, but only to a request that is not already denied.
    ///
    /// The checks come from the versioned registry
    /// ([`pod_security_policy::CheckRegistry`]): a `-version` label selects
    /// the check set that applied at that policy version.
    pub async fn admit_outcome<S: Storage>(
        &self,
        storage: &Arc<S>,
        namespace: &str,
        pod: &Pod,
        username: &str,
    ) -> Result<PodSecurityOutcome, rusternetes_common::Error> {
        let exempt = |reason: &str| PodSecurityOutcome {
            warnings: Vec::new(),
            audit_annotations: BTreeMap::from([("exempt".to_string(), reason.to_string())]),
        };
        // ValidatePod short-circuits on exempt namespaces, then users
        // (admission.go:334-343); EvaluatePod on exempt runtime classes
        // (admission.go:457-461).
        if self.exemptions.exempt_namespace(namespace) {
            return Ok(exempt("namespace"));
        }
        if self.exemptions.exempt_user(username) {
            return Ok(exempt("user"));
        }
        let ns_key = rusternetes_storage::build_key("namespaces", None, namespace);
        // admission.go:344-350: a namespace that cannot be fetched answers
        // `NewInternalError("failed to lookup namespace %q")`, not an allow.
        //
        // Deliberate deviation for NotFound: upstream never sees it here
        // because the NamespaceLifecycle plugin runs first and answers
        // NotFound for a pod in a namespace that does not exist. This server
        // has no such plugin for pod creates yet, so a missing namespace is
        // still treated as unlabelled (privileged) rather than turning every
        // pod create into a 500.
        let labels = match storage
            .get::<rusternetes_common::resources::Namespace>(&ns_key)
            .await
        {
            Ok(ns) => ns.metadata.labels,
            Err(rusternetes_common::Error::NotFound(_)) => None,
            Err(e) => {
                warn!("PodSecurity: failed to fetch pod namespace {namespace:?}: {e}");
                return Err(rusternetes_common::Error::Internal(format!(
                    "Internal error occurred: failed to lookup namespace {namespace:?}"
                )));
            }
        };
        let (policy, policy_errs) = pod_security_api::policy_to_evaluate(
            labels.as_ref(),
            pod_security_api::Policy::PRIVILEGED,
        );
        // Short-circuit on privileged enforce+audit+warn namespaces
        // (admission.go:353-357).
        if policy_errs.is_empty() && policy.fully_privileged() {
            return Ok(PodSecurityOutcome {
                warnings: Vec::new(),
                audit_annotations: BTreeMap::from([(
                    "enforce-policy".to_string(),
                    pod_security_api::LevelVersion::new(
                        pod_security_api::Level::Privileged,
                        pod_security_api::Version::LATEST,
                    )
                    .to_string(),
                )]),
            });
        }
        if self.exemptions.exempt_runtime_class(
            pod.spec
                .as_ref()
                .and_then(|s| s.runtime_class_name.as_deref()),
        ) {
            return Ok(exempt("runtimeClass"));
        }

        let mut annotations = BTreeMap::new();
        if !policy_errs.is_empty() {
            let joined = policy_errs
                .iter()
                .map(|e| e.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            annotations.insert(
                "error".to_string(),
                format!("Failed to parse policy: [{joined}]"),
            );
        }
        annotations.insert("enforce-policy".to_string(), policy.enforce.to_string());

        let pod_name = &pod.metadata.name;
        // EvaluatePod caches results by LevelVersion (admission.go:476-477).
        let default_spec = rusternetes_common::resources::pod::PodSpec::default();
        let spec = pod.spec.as_ref().unwrap_or(&default_spec);
        let mut cache: HashMap<pod_security_api::LevelVersion, Option<String>> = HashMap::new();
        let mut eval = |lv: pod_security_api::LevelVersion| {
            cache
                .entry(lv)
                .or_insert_with(|| {
                    let result = pod_security_policy::aggregate_check_results(
                        &pod_security_registry().evaluate_pod(lv, &pod.metadata, spec),
                    );
                    (!result.allowed).then(|| result.forbidden_detail())
                })
                .clone()
        };

        if let Some(detail) = eval(policy.enforce) {
            return Err(rusternetes_common::Error::Forbidden(format!(
                "pod {pod_name} violates PodSecurity \"{}\": {detail}",
                policy.enforce
            )));
        }
        if let Some(detail) = eval(policy.audit) {
            annotations.insert(
                "audit-violations".to_string(),
                format!("would violate PodSecurity \"{}\": {detail}", policy.audit),
            );
        }
        let mut warnings = Vec::new();
        if let Some(detail) = eval(policy.warn) {
            warnings.push(format!(
                "would violate PodSecurity \"{}\": {detail}",
                policy.warn
            ));
        }
        Ok(PodSecurityOutcome {
            warnings,
            audit_annotations: annotations,
        })
    }
}

/// What an admitted pod carries out of PodSecurity: the `Warn` mode's
/// warnings (an `AdmissionResponse.Warnings`) and the audit annotations
/// (`AdmissionResponse.AuditAnnotations`).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PodSecurityOutcome {
    pub warnings: Vec<String>,
    pub audit_annotations: BTreeMap<String, String>,
}

/// The check registry for the default checks, built once
/// (`policy.NewEvaluator(policy.DefaultChecks(), emulationVersion)`,
/// admission.go `CompleteConfiguration`). The emulation version is the
/// binary's 1.35, which is not older than the newest check (1.35), so it
/// does not lower the cached maximum.
fn pod_security_registry() -> &'static pod_security_policy::CheckRegistry {
    static REGISTRY: std::sync::OnceLock<pod_security_policy::CheckRegistry> =
        std::sync::OnceLock::new();
    REGISTRY.get_or_init(|| {
        pod_security_policy::CheckRegistry::new(pod_security_policy::default_checks(), None)
            .expect("the default PodSecurity checks are valid")
    })
}

/// `Priority.getDefaultPriorityClass`
/// (plugin/pkg/admission/priority/admission.go:268-285): the class marked
/// `globalDefault`. If a race left more than one, the lowest value wins.
pub async fn get_default_priority_class<S: Storage>(
    storage: &Arc<S>,
) -> rusternetes_common::Result<Option<rusternetes_common::resources::PriorityClass>> {
    let prefix = rusternetes_storage::build_prefix("priorityclasses", None);
    let list = storage
        .list::<rusternetes_common::resources::PriorityClass>(&prefix)
        .await?;
    let mut default: Option<rusternetes_common::resources::PriorityClass> = None;
    for pc in list {
        if pc.global_default == Some(true) && default.as_ref().is_none_or(|d| d.value > pc.value) {
            default = Some(pc);
        }
    }
    Ok(default)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::quota;
    use rusternetes_common::types::ObjectMeta;

    fn make_pod(name: &str, cpu_request: Option<&str>, cpu_limit: Option<&str>) -> Pod {
        let mut resources = serde_json::Map::new();
        if let Some(cpu) = cpu_request {
            resources.insert("requests".to_string(), serde_json::json!({"cpu": cpu}));
        }
        if let Some(cpu) = cpu_limit {
            resources.insert("limits".to_string(), serde_json::json!({"cpu": cpu}));
        }
        let resources_json = if resources.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::Value::Object(resources)
        };
        let pod_json = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": name},
            "spec": {
                "containers": [{
                    "name": "main",
                    "image": "busybox",
                    "resources": resources_json
                }]
            }
        });
        serde_json::from_value(pod_json).unwrap()
    }

    #[test]
    fn test_parse_cpu_to_millicores_various() {
        assert_eq!(parse_cpu_to_millicores("100m").unwrap(), 100);
        assert_eq!(parse_cpu_to_millicores("1").unwrap(), 1000);
        assert_eq!(parse_cpu_to_millicores("0.5").unwrap(), 500);
        assert_eq!(parse_cpu_to_millicores("250m").unwrap(), 250);
        assert_eq!(parse_cpu_to_millicores("2").unwrap(), 2000);
    }

    #[test]
    fn test_parse_memory_to_bytes_various() {
        assert_eq!(parse_memory_to_bytes("0").unwrap(), 0);
        assert_eq!(parse_memory_to_bytes("1024").unwrap(), 1024);
        assert_eq!(parse_memory_to_bytes("1Ki").unwrap(), 1024);
        assert_eq!(parse_memory_to_bytes("1Mi").unwrap(), 1024 * 1024);
        assert_eq!(parse_memory_to_bytes("1Gi").unwrap(), 1024 * 1024 * 1024);
    }

    /// A `ResourceQuota` or `LimitRange` may carry any quantity the API
    /// accepts. These all failed the old `trim_end_matches` chain, and in the
    /// quota path a failed parse means `i64::MAX` — the dimension is simply not
    /// enforced.
    #[test]
    fn parse_quantities_covers_full_grammar() {
        for (value, expected) in [
            ("1Ti", 1_099_511_627_776i64),
            ("1Pi", 1_125_899_906_842_624),
            ("1Ei", 1_152_921_504_606_846_976),
            ("1T", 1_000_000_000_000),
            ("1P", 1_000_000_000_000_000),
            ("1E", 1_000_000_000_000_000_000),
            ("129e6", 129_000_000),
            ("0.5", 1),
        ] {
            assert_eq!(
                parse_memory_to_bytes(value).unwrap_or_else(|e| panic!("{value}: {e}")),
                expected,
                "memory {value:?}"
            );
        }
        // Sub-unit CPU suffixes, rounded up as `MilliValue()` does.
        assert_eq!(parse_cpu_to_millicores("0.5m").unwrap(), 1);
        assert_eq!(parse_cpu_to_millicores("10.5m").unwrap(), 11);
        assert_eq!(parse_cpu_to_millicores("1500u").unwrap(), 2);
    }

    /// `k` is the kilo suffix; `K` is not in the grammar at all. The chain
    /// matched only `ends_with("K")`, so it had these exactly backwards.
    #[test]
    fn parse_memory_accepts_lowercase_k_and_rejects_uppercase() {
        assert_eq!(parse_memory_to_bytes("1k").unwrap(), 1_000);
        assert!(parse_memory_to_bytes("1K").is_err());
    }

    /// `trim_end_matches` strips *every* trailing occurrence of the suffix.
    #[test]
    fn parse_memory_rejects_repeated_suffix() {
        assert!(parse_memory_to_bytes("1GiGi").is_err());
        assert!(parse_memory_to_bytes("1MiMi").is_err());
        assert!(parse_cpu_to_millicores("100mm").is_err());
    }

    /// An unparseable `spec.hard` value leaves that one dimension unenforced
    /// without taking the rest of the quota with it. The old code reached the
    /// same outcome via a per-key `unwrap_or(i64::MAX)`; now there is one place
    /// where a bad quantity is dropped, and the surviving keys still bind.
    #[test]
    fn unparseable_hard_value_drops_only_its_own_dimension() {
        let mut hard = HashMap::new();
        hard.insert("requests.cpu".to_string(), "1".to_string());
        hard.insert("requests.memory".to_string(), "nonsense".to_string());
        let parsed = quota::parse_resource_list(&hard);
        assert_eq!(quota::resource_names(&parsed), vec!["requests.cpu"]);
    }

    // ---- LimitRange admission tests ----

    fn make_limit_range(
        default_cpu: Option<&str>,
        default_request_cpu: Option<&str>,
        min_cpu: Option<&str>,
        max_cpu: Option<&str>,
    ) -> LimitRange {
        let mut default = HashMap::new();
        if let Some(v) = default_cpu {
            default.insert("cpu".to_string(), v.to_string());
        }
        let mut default_request = HashMap::new();
        if let Some(v) = default_request_cpu {
            default_request.insert("cpu".to_string(), v.to_string());
        }
        let mut min = HashMap::new();
        if let Some(v) = min_cpu {
            min.insert("cpu".to_string(), v.to_string());
        }
        let mut max = HashMap::new();
        if let Some(v) = max_cpu {
            max.insert("cpu".to_string(), v.to_string());
        }
        LimitRange {
            type_meta: rusternetes_common::types::TypeMeta {
                api_version: "v1".to_string(),
                kind: "LimitRange".to_string(),
            },
            metadata: ObjectMeta::new("test-limit-range").with_namespace("default"),
            spec: rusternetes_common::resources::LimitRangeSpec {
                limits: vec![rusternetes_common::resources::LimitRangeItem {
                    item_type: "Container".to_string(),
                    default: if default.is_empty() {
                        None
                    } else {
                        Some(default)
                    },
                    default_request: if default_request.is_empty() {
                        None
                    } else {
                        Some(default_request)
                    },
                    min: if min.is_empty() { None } else { Some(min) },
                    max: if max.is_empty() { None } else { Some(max) },
                    max_limit_request_ratio: None,
                }],
            },
        }
    }

    #[tokio::test]
    async fn test_limit_range_applies_default_request_cpu() {
        // Conformance test scenario: LimitRange with default=500m, defaultRequest=300m
        // Pod with NO resources should get requests.cpu=300m, limits.cpu=500m
        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        let lr = make_limit_range(Some("500m"), Some("300m"), Some("100m"), Some("1"));
        let lr_key = "/registry/limitranges/default/test-limit-range";
        storage.create(lr_key, &lr).await.unwrap();

        let mut pod = make_pod("test-pod", None, None);
        let result = apply_limit_range(&storage, "default", &mut pod)
            .await
            .unwrap();
        assert!(result, "LimitRange admission should pass");

        let resources = pod.spec.as_ref().unwrap().containers[0]
            .resources
            .as_ref()
            .unwrap();
        let requests = resources.requests.as_ref().expect("requests should be set");
        let limits = resources.limits.as_ref().expect("limits should be set");

        assert_eq!(
            requests.get("cpu").unwrap(),
            "300m",
            "requests.cpu should be 300m from defaultRequest, not from default limits"
        );
        assert_eq!(
            limits.get("cpu").unwrap(),
            "500m",
            "limits.cpu should be 500m from default"
        );
    }

    #[tokio::test]
    async fn test_limit_range_requests_fallback_to_limits_when_no_default_request() {
        // When defaultRequest is NOT set but default (limits) IS set,
        // requests should default to the limit value
        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        let lr = make_limit_range(Some("500m"), None, None, None);
        let lr_key = "/registry/limitranges/default/test-limit-range";
        storage.create(lr_key, &lr).await.unwrap();

        let mut pod = make_pod("test-pod", None, None);
        let result = apply_limit_range(&storage, "default", &mut pod)
            .await
            .unwrap();
        assert!(result);

        let resources = pod.spec.as_ref().unwrap().containers[0]
            .resources
            .as_ref()
            .unwrap();
        let requests = resources.requests.as_ref().expect("requests should be set");
        let limits = resources.limits.as_ref().expect("limits should be set");

        assert_eq!(limits.get("cpu").unwrap(), "500m");
        assert_eq!(
            requests.get("cpu").unwrap(),
            "500m",
            "requests.cpu should fall back to default limits (500m) when defaultRequest not set"
        );
    }

    #[tokio::test]
    async fn test_limit_range_does_not_override_explicit_requests() {
        // Container with explicit requests.cpu=200m should NOT be overridden by defaultRequest
        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        let lr = make_limit_range(Some("500m"), Some("300m"), Some("100m"), Some("1"));
        let lr_key = "/registry/limitranges/default/test-limit-range";
        storage.create(lr_key, &lr).await.unwrap();

        let mut pod = make_pod("test-pod", Some("200m"), None);
        let result = apply_limit_range(&storage, "default", &mut pod)
            .await
            .unwrap();
        assert!(result);

        let resources = pod.spec.as_ref().unwrap().containers[0]
            .resources
            .as_ref()
            .unwrap();
        let requests = resources.requests.as_ref().expect("requests should be set");
        assert_eq!(
            requests.get("cpu").unwrap(),
            "200m",
            "explicit requests.cpu=200m should not be overridden by defaultRequest=300m"
        );
    }

    #[tokio::test]
    async fn test_limit_range_limits_default_to_requests_when_unset() {
        // K8s rule: if limits are set (from LimitRange default) but container has no requests,
        // requests should default to the limits value
        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        // Only default limits, no defaultRequest
        let lr = make_limit_range(Some("400m"), None, None, None);
        let lr_key = "/registry/limitranges/default/test-limit-range";
        storage.create(lr_key, &lr).await.unwrap();

        let mut pod = make_pod("test-pod", None, None);
        let result = apply_limit_range(&storage, "default", &mut pod)
            .await
            .unwrap();
        assert!(result);

        let resources = pod.spec.as_ref().unwrap().containers[0]
            .resources
            .as_ref()
            .unwrap();
        let requests = resources.requests.as_ref().expect("requests should be set");
        let limits = resources.limits.as_ref().expect("limits should be set");

        assert_eq!(limits.get("cpu").unwrap(), "400m");
        assert_eq!(
            requests.get("cpu").unwrap(),
            "400m",
            "requests.cpu should default to limits.cpu when no defaultRequest"
        );
    }

    #[tokio::test]
    async fn test_limit_range_explicit_limits_override_default_request() {
        // Conformance scenario: pod has explicit limits.cpu=300m but no requests.cpu.
        // LimitRange has default=500m, defaultRequest=100m.
        // The pod has explicit limits.cpu=300m but no requests.cpu.
        // apply_limit_range only handles LimitRange defaults — the pod-level
        // limits→requests defaulting happens in the pod handler BEFORE this.
        // So apply_limit_range should set requests.cpu = 100m (from defaultRequest).
        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        let lr = make_limit_range(Some("500m"), Some("100m"), Some("50m"), Some("1"));
        let lr_key = "/registry/limitranges/default/test-limit-range";
        storage.create(lr_key, &lr).await.unwrap();

        let mut pod = make_pod("test-pod", None, Some("300m"));
        let result = apply_limit_range(&storage, "default", &mut pod)
            .await
            .unwrap();
        assert!(result);

        let resources = pod.spec.as_ref().unwrap().containers[0]
            .resources
            .as_ref()
            .unwrap();
        let requests = resources.requests.as_ref().expect("requests should be set");
        let limits = resources.limits.as_ref().expect("limits should be set");

        assert_eq!(
            limits.get("cpu").unwrap(),
            "300m",
            "explicit limits.cpu=300m should be preserved"
        );
        // Note: the pod handler does limits→requests defaulting BEFORE calling
        // apply_limit_range, so in production requests.cpu=300m. But this unit
        // test only calls apply_limit_range, which applies defaultRequest=100m.
        assert_eq!(
            requests.get("cpu").unwrap(),
            "100m",
            "apply_limit_range sets requests from defaultRequest (pod handler does limits->requests)"
        );
    }

    #[test]
    fn test_validate_max_rejects_over_limit_cpu() {
        let resources = ResourceRequirements {
            limits: Some({
                let mut m = HashMap::new();
                m.insert("cpu".to_string(), "800m".to_string());
                m
            }),
            requests: None,
            claims: None,
        };
        let max = {
            let mut m = HashMap::new();
            m.insert("cpu".to_string(), "500m".to_string());
            m
        };
        let result = validate_max_resources(&resources, &max, "test").unwrap();
        assert!(!result, "800m CPU should exceed max of 500m");
    }

    #[test]
    fn test_validate_max_rejects_over_limit_memory() {
        let resources = ResourceRequirements {
            limits: Some({
                let mut m = HashMap::new();
                m.insert("memory".to_string(), "1Gi".to_string());
                m
            }),
            requests: None,
            claims: None,
        };
        let max = {
            let mut m = HashMap::new();
            m.insert("memory".to_string(), "500Mi".to_string());
            m
        };
        let result = validate_max_resources(&resources, &max, "test").unwrap();
        assert!(!result, "1Gi memory should exceed max of 500Mi");
    }

    #[test]
    fn test_validate_max_rejects_over_limit_ephemeral_storage() {
        let resources = ResourceRequirements {
            limits: Some({
                let mut m = HashMap::new();
                m.insert("ephemeral-storage".to_string(), "2Gi".to_string());
                m
            }),
            requests: None,
            claims: None,
        };
        let max = {
            let mut m = HashMap::new();
            m.insert("ephemeral-storage".to_string(), "1Gi".to_string());
            m
        };
        let result = validate_max_resources(&resources, &max, "test").unwrap();
        assert!(!result, "2Gi ephemeral-storage should exceed max of 1Gi");
    }

    #[test]
    fn test_validate_max_checks_requests_too() {
        let resources = ResourceRequirements {
            limits: None,
            requests: Some({
                let mut m = HashMap::new();
                m.insert("cpu".to_string(), "800m".to_string());
                m
            }),
            claims: None,
        };
        let max = {
            let mut m = HashMap::new();
            m.insert("cpu".to_string(), "500m".to_string());
            m
        };
        let result = validate_max_resources(&resources, &max, "test").unwrap();
        assert!(!result, "800m CPU request should exceed max of 500m");
    }

    #[test]
    fn test_validate_max_allows_within_limit() {
        let resources = ResourceRequirements {
            limits: Some({
                let mut m = HashMap::new();
                m.insert("cpu".to_string(), "400m".to_string());
                m
            }),
            requests: None,
            claims: None,
        };
        let max = {
            let mut m = HashMap::new();
            m.insert("cpu".to_string(), "500m".to_string());
            m
        };
        let result = validate_max_resources(&resources, &max, "test").unwrap();
        assert!(result, "400m CPU should be within max of 500m");
    }

    // ----- PodSecurityAdmission allow-case coverage -----
    //
    // The reject paths are pinned by the HTTP-level tests in
    // `tests/pod_security_admission_test.rs`. These unit tests cover the
    // admit (allow) paths the integration tests don't assert.

    async fn put_namespace<S: Storage>(storage: &Arc<S>, name: &str, enforce: Option<&str>) {
        let mut labels = std::collections::BTreeMap::new();
        if let Some(level) = enforce {
            labels.insert(
                "pod-security.kubernetes.io/enforce".to_string(),
                level.to_string(),
            );
        }
        let ns = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": { "name": name, "labels": labels },
        });
        let ns: rusternetes_common::resources::Namespace = serde_json::from_value(ns).unwrap();
        let key = rusternetes_storage::build_key("namespaces", None, name);
        storage.create(&key, &ns).await.unwrap();
    }

    fn pod_from_spec(name: &str, spec: serde_json::Value) -> Pod {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": { "name": name },
            "spec": spec,
        }))
        .unwrap()
    }

    #[tokio::test]
    async fn psa_privileged_namespace_allows_privileged_pod() {
        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        put_namespace(&storage, "ns", Some("privileged")).await;
        let pod = pod_from_spec(
            "p",
            serde_json::json!({
                "hostPID": true,
                "containers": [{
                    "name": "main", "image": "busybox",
                    "securityContext": { "privileged": true },
                }],
            }),
        );
        PodSecurityAdmission::new()
            .admit_as(&storage, "ns", &pod, "")
            .await
            .expect("privileged namespace must admit everything");
    }

    fn psa_pod(spec: serde_json::Value) -> Pod {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": "p"}, "spec": spec
        }))
        .unwrap()
    }

    /// admission_test.go `TestValidatePodAndController` update cases /
    /// `isSignificantPodUpdate` (admission.go:632-666).
    #[test]
    fn psa_is_significant_pod_update() {
        let base = psa_pod(serde_json::json!({
            "containers": [{"name": "a", "image": "i1"}],
            "initContainers": [{"name": "i", "image": "j1"}]
        }));
        assert!(!is_significant_pod_update(&base, &base));
        let image = psa_pod(serde_json::json!({
            "containers": [{"name": "a", "image": "i2"}],
            "initContainers": [{"name": "i", "image": "j1"}]
        }));
        assert!(is_significant_pod_update(&image, &base));
        let init_image = psa_pod(serde_json::json!({
            "containers": [{"name": "a", "image": "i1"}],
            "initContainers": [{"name": "i", "image": "j2"}]
        }));
        assert!(is_significant_pod_update(&init_image, &base));
        let added = psa_pod(serde_json::json!({
            "containers": [{"name": "a", "image": "i1"}, {"name": "b", "image": "i1"}],
            "initContainers": [{"name": "i", "image": "j1"}]
        }));
        assert!(is_significant_pod_update(&added, &base));
        let eph = psa_pod(serde_json::json!({
            "containers": [{"name": "a", "image": "i1"}],
            "initContainers": [{"name": "i", "image": "j1"}],
            "ephemeralContainers": [{"name": "e", "image": "x"}]
        }));
        assert!(is_significant_pod_update(&eph, &base));
        assert!(!is_significant_pod_update(&eph, &eph));
        let eph_image = psa_pod(serde_json::json!({
            "containers": [{"name": "a", "image": "i1"}],
            "initContainers": [{"name": "i", "image": "j1"}],
            "ephemeralContainers": [{"name": "e", "image": "y"}]
        }));
        assert!(is_significant_pod_update(&eph_image, &eph));
    }

    /// `ignoredPodSubresources` (admission.go:316-325).
    #[test]
    fn psa_ignored_subresources_are_not_evaluated() {
        let pod = psa_pod(serde_json::json!({"containers": [{"name": "a", "image": "i"}]}));
        for s in IGNORED_POD_SUBRESOURCES {
            assert!(!PodSecurityAdmission::should_evaluate(Some(s), None, &pod));
        }
        for s in [None, Some("ephemeralcontainers"), Some("resize")] {
            assert!(PodSecurityAdmission::should_evaluate(s, None, &pod));
        }
        // An insignificant update is allowed unevaluated.
        assert!(!PodSecurityAdmission::should_evaluate(
            None,
            Some(&pod),
            &pod
        ));
    }

    #[tokio::test]
    async fn psa_missing_enforce_label_allows() {
        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        put_namespace(&storage, "ns", None).await;
        let pod = pod_from_spec(
            "p",
            serde_json::json!({
                "containers": [{
                    "name": "main", "image": "busybox",
                    "securityContext": { "privileged": true },
                }],
            }),
        );
        PodSecurityAdmission::new()
            .admit_as(&storage, "ns", &pod, "")
            .await
            .expect("absent enforce label must admit everything");
    }

    #[tokio::test]
    async fn psa_restricted_admits_compliant_pod() {
        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        put_namespace(&storage, "ns", Some("restricted")).await;
        let pod = pod_from_spec(
            "p",
            serde_json::json!({
                "securityContext": {
                    "runAsNonRoot": true,
                    "seccompProfile": { "type": "RuntimeDefault" },
                },
                "volumes": [{ "name": "data", "emptyDir": {} }],
                "containers": [{
                    "name": "main", "image": "busybox",
                    "securityContext": {
                        "runAsNonRoot": true,
                        "runAsUser": 1000,
                        "allowPrivilegeEscalation": false,
                        "capabilities": { "drop": ["ALL"] },
                    },
                }],
            }),
        );
        PodSecurityAdmission::new()
            .admit_as(&storage, "ns", &pod, "")
            .await
            .expect("compliant restricted pod must be admitted");
    }

    // ---- PodSecurity exemptions (admission_test.go TestValidatePodAndController
    // "exempt namespace" / "exempt user" / "exempt runtimeClass") ----

    fn exemptions() -> PodSecurityExemptions {
        PodSecurityExemptions {
            usernames: vec!["exempt-user".into()],
            namespaces: vec!["exempt-ns".into()],
            runtime_classes: vec!["exempt-rc".into()],
        }
    }

    fn privileged_pod(runtime_class: Option<&str>) -> Pod {
        let mut spec = serde_json::json!({
            "containers": [{
                "name": "main", "image": "busybox",
                "securityContext": { "privileged": true },
            }],
        });
        if let Some(rc) = runtime_class {
            spec["runtimeClassName"] = serde_json::json!(rc);
        }
        pod_from_spec("p", spec)
    }

    #[tokio::test]
    async fn psa_exempt_namespace_admits_violating_pod() {
        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        put_namespace(&storage, "exempt-ns", Some("restricted")).await;
        put_namespace(&storage, "other-ns", Some("restricted")).await;
        let psa = PodSecurityAdmission::with_exemptions(exemptions());
        psa.admit_as(&storage, "exempt-ns", &privileged_pod(None), "alice")
            .await
            .expect("exempt namespace admits");
        psa.admit_as(&storage, "other-ns", &privileged_pod(None), "alice")
            .await
            .expect_err("non-exempt namespace still enforced");
    }

    #[tokio::test]
    async fn psa_exempt_user_admits_violating_pod() {
        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        put_namespace(&storage, "ns", Some("restricted")).await;
        let psa = PodSecurityAdmission::with_exemptions(exemptions());
        psa.admit_as(&storage, "ns", &privileged_pod(None), "exempt-user")
            .await
            .expect("exempt user admits");
        psa.admit_as(&storage, "ns", &privileged_pod(None), "alice")
            .await
            .expect_err("non-exempt user still enforced");
        psa.admit_as(&storage, "ns", &privileged_pod(None), "")
            .await
            .expect_err("empty username is never exempt");
    }

    #[tokio::test]
    async fn psa_exempt_runtime_class_admits_violating_pod() {
        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        put_namespace(&storage, "ns", Some("restricted")).await;
        let psa = PodSecurityAdmission::with_exemptions(exemptions());
        psa.admit_as(&storage, "ns", &privileged_pod(Some("exempt-rc")), "alice")
            .await
            .expect("exempt runtimeClass admits");
        psa.admit_as(&storage, "ns", &privileged_pod(Some("other-rc")), "alice")
            .await
            .expect_err("other runtimeClass still enforced");
    }

    #[test]
    fn psa_exemptions_parse_from_admission_configuration() {
        let yaml = r#"
apiVersion: apiserver.config.k8s.io/v1
kind: AdmissionConfiguration
plugins:
- name: PodSecurity
  configuration:
    apiVersion: pod-security.admission.config.k8s.io/v1
    kind: PodSecurityConfiguration
    defaults:
      enforce: baseline
    exemptions:
      usernames: ["u"]
      namespaces: ["n1", "n2"]
      runtimeClasses: ["rc"]
"#;
        let e = PodSecurityExemptions::from_admission_configuration(yaml).unwrap();
        assert_eq!(e.usernames, vec!["u"]);
        assert_eq!(e.namespaces, vec!["n1", "n2"]);
        assert_eq!(e.runtime_classes, vec!["rc"]);
        assert_eq!(
            PodSecurityExemptions::from_admission_configuration("plugins: []").unwrap(),
            PodSecurityExemptions::default()
        );
    }

    // ---- PodSecurity audit / warn modes and label parsing ----
    // admission_test.go `TestValidatePodAndController` cases "enforce deny",
    // "warn deny", "audit deny", "invalid namespace labels", exempt and
    // privileged short-circuits (annotation keys).

    /// `makeNs(enforce, warn, audit)` (admission_test.go).
    async fn put_ns_modes(
        storage: &Arc<rusternetes_storage::MemoryStorage>,
        name: &str,
        enforce: &str,
        warn: &str,
        audit: &str,
    ) {
        let mut labels = std::collections::BTreeMap::new();
        for (k, v) in [("enforce", enforce), ("warn", warn), ("audit", audit)] {
            if !v.is_empty() {
                labels.insert(format!("pod-security.kubernetes.io/{k}"), v.to_string());
            }
        }
        let ns: rusternetes_common::resources::Namespace =
            serde_json::from_value(serde_json::json!({"apiVersion": "v1", "kind": "Namespace",
                "metadata": {"name": name, "labels": labels}}))
            .unwrap();
        let key = rusternetes_storage::build_key("namespaces", None, name);
        storage.create(&key, &ns).await.unwrap();
    }

    fn baseline_pod() -> Pod {
        pod_from_spec(
            "p",
            serde_json::json!({"containers": [{"name": "main", "image": "busybox"}]}),
        )
    }

    #[tokio::test]
    async fn psa_warn_deny_allows_with_warning() {
        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        put_ns_modes(&storage, "warn-ns", "", "baseline", "").await;
        let out = PodSecurityAdmission::new()
            .admit_outcome(&storage, "warn-ns", &privileged_pod(None), "alice")
            .await
            .expect("warn mode never denies");
        assert_eq!(out.warnings.len(), 1, "{:?}", out.warnings);
        assert!(
            out.warnings[0].starts_with(r#"would violate PodSecurity "baseline:latest": "#),
            "{}",
            out.warnings[0]
        );
        assert!(!out.audit_annotations.contains_key("audit-violations"));
    }

    #[tokio::test]
    async fn psa_audit_deny_allows_with_annotation() {
        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        put_ns_modes(&storage, "audit-ns", "", "", "baseline").await;
        let out = PodSecurityAdmission::new()
            .admit_outcome(&storage, "audit-ns", &privileged_pod(None), "alice")
            .await
            .expect("audit mode never denies");
        assert!(out.warnings.is_empty());
        let v = &out.audit_annotations["audit-violations"];
        assert!(
            v.starts_with(r#"would violate PodSecurity "baseline:latest": "#),
            "{v}"
        );
    }

    /// "enforce deny": no warning on a request already rejected; the audit
    /// annotation records the enforced policy.
    #[tokio::test]
    async fn psa_enforce_deny_message_names_level_and_version() {
        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        put_ns_modes(&storage, "r-ns", "restricted", "", "").await;
        let err = PodSecurityAdmission::new()
            .admit_outcome(&storage, "r-ns", &privileged_pod(None), "alice")
            .await
            .expect_err("enforce denies");
        assert!(
            err.to_string()
                .contains(r#"violates PodSecurity "restricted:latest": "#),
            "{err}"
        );
    }

    /// "enforce allow" records `enforce-policy`; the privileged
    /// short-circuit is `privileged:latest` (response.go init).
    #[tokio::test]
    async fn psa_enforce_policy_audit_annotation() {
        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        put_ns_modes(&storage, "b-ns", "baseline", "", "").await;
        put_ns_modes(&storage, "p-ns", "", "", "").await;
        let psa = PodSecurityAdmission::new();
        let out = psa
            .admit_outcome(&storage, "b-ns", &baseline_pod(), "alice")
            .await
            .unwrap();
        assert_eq!(out.audit_annotations["enforce-policy"], "baseline:latest");
        assert!(out.warnings.is_empty());
        let out = psa
            .admit_outcome(&storage, "p-ns", &privileged_pod(None), "alice")
            .await
            .unwrap();
        assert_eq!(out.audit_annotations["enforce-policy"], "privileged:latest");
    }

    /// "invalid namespace labels": an unparseable enforce level is
    /// restricted:latest, so even a baseline pod is denied, and the parse
    /// error is annotated (EvaluatePod :466-470).
    #[tokio::test]
    async fn psa_invalid_enforce_label_fails_closed() {
        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        put_ns_modes(&storage, "invalid-ns", "not-a-valid-level", "", "").await;
        let err = PodSecurityAdmission::new()
            .admit_outcome(&storage, "invalid-ns", &baseline_pod(), "alice")
            .await
            .expect_err("restricted:latest denies a pod without runAsNonRoot");
        assert!(err.to_string().contains("restricted:latest"), "{err}");
    }

    /// An unparseable audit/warn level fails open and is annotated.
    #[tokio::test]
    async fn psa_invalid_warn_label_fails_open_with_error_annotation() {
        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        put_ns_modes(&storage, "w-ns", "", "bogus", "").await;
        let out = PodSecurityAdmission::new()
            .admit_outcome(&storage, "w-ns", &privileged_pod(None), "alice")
            .await
            .unwrap();
        assert!(out.warnings.is_empty());
        assert!(
            out.audit_annotations["error"].starts_with("Failed to parse policy: "),
            "{:?}",
            out.audit_annotations
        );
    }

    /// Enforce also defaults warn to the enforce level, but a request that is
    /// already denied carries no warning.
    #[tokio::test]
    async fn psa_enforce_defaults_warn_level() {
        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        put_ns_modes(&storage, "e-ns", "baseline", "", "").await;
        // restricted audit sees the baseline pod's missing runAsNonRoot.
        put_ns_modes(&storage, "ea-ns", "baseline", "", "restricted").await;
        let out = PodSecurityAdmission::new()
            .admit_outcome(&storage, "ea-ns", &baseline_pod(), "alice")
            .await
            .unwrap();
        assert!(out.audit_annotations.contains_key("audit-violations"));
        assert!(out.warnings.is_empty(), "warn defaults to baseline: passes");
    }

    /// Exemptions annotate the reason (response.go init).
    #[tokio::test]
    async fn psa_exemption_audit_annotation() {
        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        put_ns_modes(&storage, "ns", "restricted", "", "").await;
        let psa = PodSecurityAdmission::with_exemptions(exemptions());
        let by = |ns: &'static str, user: &'static str, rc: Option<&'static str>| {
            let psa = psa.clone();
            let storage = storage.clone();
            async move {
                psa.admit_outcome(&storage, ns, &privileged_pod(rc), user)
                    .await
                    .unwrap()
                    .audit_annotations["exempt"]
                    .clone()
            }
        };
        assert_eq!(by("exempt-ns", "alice", None).await, "namespace");
        assert_eq!(by("ns", "exempt-user", None).await, "user");
        assert_eq!(by("ns", "alice", Some("exempt-rc")).await, "runtimeClass");
    }

    // ---- imagePullSecrets propagation (SA admission, upstream parity) -------

    async fn put_sa<S: Storage>(storage: &Arc<S>, ns: &str, name: &str, secrets: &[&str]) {
        let sa: ServiceAccount = serde_json::from_value(serde_json::json!({
            "apiVersion": "v1",
            "kind": "ServiceAccount",
            "metadata": {"name": name, "namespace": ns},
            "imagePullSecrets": secrets.iter().map(|s| serde_json::json!({"name": s})).collect::<Vec<_>>(),
        }))
        .unwrap();
        let key = format!("/registry/serviceaccounts/{}/{}", ns, name);
        storage.create(&key, &sa).await.unwrap();
    }

    fn pull_secret_names(pod: &Pod) -> Vec<String> {
        pod.spec
            .as_ref()
            .and_then(|s| s.image_pull_secrets.as_ref())
            .map(|v| v.iter().map(|r| r.name.clone()).collect())
            .unwrap_or_default()
    }

    #[tokio::test]
    async fn imagepullsecrets_propagated_from_sa_when_pod_has_none() {
        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        put_sa(&storage, "ns", "default", &["regcred", "ghcr"]).await;
        let mut pod = make_pod("p", None, None);
        inject_service_account_token(&storage, "ns", &mut pod)
            .await
            .unwrap();
        assert_eq!(pull_secret_names(&pod), vec!["regcred", "ghcr"]);
    }

    #[tokio::test]
    async fn imagepullsecrets_pod_list_wins_no_merge() {
        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        put_sa(&storage, "ns", "default", &["regcred"]).await;
        let mut pod = make_pod("p", None, None);
        // Pod already declares its own secret — SA list must NOT be appended.
        pod.spec.as_mut().unwrap().image_pull_secrets = Some(vec![
            rusternetes_common::resources::pod::LocalObjectReference {
                name: "pod-own".to_string(),
            },
        ]);
        inject_service_account_token(&storage, "ns", &mut pod)
            .await
            .unwrap();
        assert_eq!(pull_secret_names(&pod), vec!["pod-own"]);
    }

    #[tokio::test]
    async fn imagepullsecrets_noop_when_sa_has_none() {
        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        put_sa(&storage, "ns", "default", &[]).await;
        let mut pod = make_pod("p", None, None);
        inject_service_account_token(&storage, "ns", &mut pod)
            .await
            .unwrap();
        assert!(pull_secret_names(&pod).is_empty());
    }

    #[tokio::test]
    async fn imagepullsecrets_propagated_even_when_automount_disabled() {
        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        put_sa(&storage, "ns", "default", &["regcred"]).await;
        let mut pod = make_pod("p", None, None);
        // automount off must not block imagePullSecrets propagation.
        pod.spec.as_mut().unwrap().automount_service_account_token = Some(false);
        inject_service_account_token(&storage, "ns", &mut pod)
            .await
            .unwrap();
        assert_eq!(pull_secret_names(&pod), vec!["regcred"]);
    }

    // --- #2493: versioned policy checks + namespace lookup failure ---

    async fn put_ns_labels<S: Storage>(storage: &Arc<S>, name: &str, labels: &[(&str, &str)]) {
        let labels: std::collections::BTreeMap<String, String> = labels
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let ns: rusternetes_common::resources::Namespace =
            serde_json::from_value(serde_json::json!({
                "apiVersion": "v1", "kind": "Namespace",
                "metadata": { "name": name, "labels": labels },
            }))
            .unwrap();
        let key = rusternetes_storage::build_key("namespaces", None, name);
        storage.create(&key, &ns).await.unwrap();
    }

    /// policy/check_sysctls.go:70-79: `net.ipv4.tcp_rmem` joined the safe
    /// set in v1.32, so `baseline:v1.31` forbids it and `latest` allows it.
    #[tokio::test]
    async fn psa_enforce_version_label_selects_older_sysctls_check() {
        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        put_ns_labels(
            &storage,
            "old",
            &[
                ("pod-security.kubernetes.io/enforce", "baseline"),
                ("pod-security.kubernetes.io/enforce-version", "v1.31"),
            ],
        )
        .await;
        put_ns_labels(
            &storage,
            "new",
            &[("pod-security.kubernetes.io/enforce", "baseline")],
        )
        .await;
        let pod = psa_pod(serde_json::json!({
            "securityContext": {"sysctls": [{"name": "net.ipv4.tcp_rmem", "value": "1 2 3"}]},
            "containers": [{"name": "a", "image": "i"}],
        }));
        let psa = PodSecurityAdmission::new();
        let err = psa
            .admit_outcome(&storage, "old", &pod, "alice")
            .await
            .expect_err("v1.31 forbids tcp_rmem");
        assert!(
            err.to_string().contains(
                r#"violates PodSecurity "baseline:v1.31": forbidden sysctls (net.ipv4.tcp_rmem)"#
            ),
            "{err}"
        );
        psa.admit_outcome(&storage, "new", &pod, "alice")
            .await
            .expect("latest allows tcp_rmem");
    }

    /// policy/check_seccompProfile_restricted.go:54 and
    /// check_capabilities_restricted.go:65: both restricted checks start at
    /// v1.19 / v1.22, so a pod that sets neither passes `restricted:v1.18`.
    #[tokio::test]
    async fn psa_restricted_old_version_lacks_newer_checks() {
        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        put_ns_labels(
            &storage,
            "old",
            &[
                ("pod-security.kubernetes.io/enforce", "restricted"),
                ("pod-security.kubernetes.io/enforce-version", "v1.18"),
            ],
        )
        .await;
        let pod = psa_pod(serde_json::json!({
            "securityContext": {"runAsNonRoot": true},
            "containers": [{"name": "a", "image": "i",
                "securityContext": {"allowPrivilegeEscalation": false}}],
        }));
        PodSecurityAdmission::new()
            .admit_outcome(&storage, "old", &pod, "alice")
            .await
            .expect("restricted:v1.18 predates seccomp and capabilities checks");
    }

    /// The enforce message lists EVERY failing check, baseline first, as
    /// `AggregateCheckResult.ForbiddenDetail` does (checks.go:98-117).
    #[tokio::test]
    async fn psa_enforce_message_aggregates_all_violations() {
        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        put_ns_labels(
            &storage,
            "ns",
            &[("pod-security.kubernetes.io/enforce", "baseline")],
        )
        .await;
        let pod = psa_pod(serde_json::json!({
            "hostNetwork": true,
            "containers": [{"name": "a", "image": "i", "securityContext": {"privileged": true},
                "ports": [{"containerPort": 80, "hostPort": 80}]}],
        }));
        let err = PodSecurityAdmission::new()
            .admit_outcome(&storage, "ns", &pod, "alice")
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.contains(r#"host namespaces (hostNetwork=true), hostPort (container "a" uses hostPort 80), privileged (container "a" must not set securityContext.privileged=true)"#),
            "{err}"
        );
    }

    /// admission.go:344-350: a namespace that cannot be read is an
    /// InternalError, not an allow.
    #[tokio::test]
    async fn psa_namespace_lookup_failure_is_internal_error() {
        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        // An unreadable (undecodable) Namespace record: a read error that is
        // not NotFound.
        let key = rusternetes_storage::build_key("namespaces", None, "broken");
        storage
            .create(&key, &serde_json::json!("not a namespace"))
            .await
            .unwrap();
        let err = PodSecurityAdmission::new()
            .admit_outcome(&storage, "broken", &baseline_pod(), "alice")
            .await
            .expect_err("an unreadable namespace must not fail open");
        assert!(
            matches!(&err, rusternetes_common::Error::Internal(m)
                if m.contains(r#"failed to lookup namespace "broken""#)),
            "{err:?}"
        );
    }

    /// A namespace that does not exist is unlabelled: NamespaceLifecycle
    /// (upstream) answers NotFound before PodSecurity runs, and this server
    /// has no such plugin for pod creates.
    #[tokio::test]
    async fn psa_missing_namespace_is_unlabelled() {
        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        PodSecurityAdmission::new()
            .admit_outcome(&storage, "missing", &privileged_pod(None), "alice")
            .await
            .expect("a missing namespace carries no labels");
    }
}
