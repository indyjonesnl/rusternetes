pub mod admission_metrics;
pub mod builtin_plugins;
pub mod caching_authorizer;
pub mod certificates;
pub mod config;
pub mod event_rate_limit;
pub mod image_policy;
pub mod namespace_plugins;
pub mod owner_references_permission_enforcement;
pub mod plugin_metrics;
pub mod plugins;
pub mod pod_plugins;
pub mod pod_security_api;
pub mod pod_security_controller;
pub mod pod_security_namespace;
pub mod pod_security_policy;
pub mod policy_apply_config;
pub mod policy_dispatch;
pub mod policy_install;
pub mod policy_json_patch;
pub mod policy_json_patch_ops;
pub mod policy_match_conditions;
pub mod policy_matching;
pub mod policy_mutating;
pub mod policy_mutating_metrics;
pub mod policy_plugin;
pub mod policy_registry;
pub mod policy_source;
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

pub fn validate_max_resources(
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
pub fn parse_cpu_to_millicores(cpu: &str) -> anyhow::Result<i64> {
    Ok(parse_resource_value(cpu, "cpu")?)
}

/// Parse a byte-denominated quantity (memory, ephemeral-storage, PVC storage)
/// into bytes. See [`parse_cpu_to_millicores`] for why this is shared.
///
/// The `trim_end_matches` chain this replaced handled `Ti`/`Pi`/`Ei`/`T`/`P`/`E`
/// nowhere, matched only an uppercase `K` — so the non-upstream `"1K"` parsed
/// while the valid `"1k"` did not — and stripped *repeated* suffixes, so
/// `"1GiGi"` read as 1Gi.
pub fn parse_memory_to_bytes(memory: &str) -> anyhow::Result<i64> {
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
pub const IGNORED_POD_SUBRESOURCES: [&str; 8] = [
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
    pub fn from_admission_configuration(
        yaml: &str,
        config_file: &std::path::Path,
    ) -> Result<Self, String> {
        let Some(text) = config::plugin_configuration_for(yaml, config_file, "PodSecurity")? else {
            return Ok(Self::default());
        };
        let cfg: serde_json::Value = serde_yaml::from_str(&text).map_err(|e| e.to_string())?;
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
        // admission.go:344-350: any namespace lookup failure, NotFound
        // included, answers `NewInternalError("failed to lookup namespace
        // %q")`, not an allow. Upstream never sees NotFound here because
        // NamespaceLifecycle answers it first; that plugin exists here too
        // (#2543), so there is no NotFound arm.
        let labels = match storage
            .get::<rusternetes_common::resources::Namespace>(&ns_key)
            .await
        {
            Ok(ns) => ns.metadata.labels,
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

/// `audit.AddAuditAnnotations(ctx, AuditAnnotationPrefix+k, v...)` for the
/// outcome's `AdmissionResponse.AuditAnnotations`
/// (plugin/pkg/admission/security/podsecurity/admission.go:203-211;
/// `AuditAnnotationPrefix` = `pod-security.kubernetes.io/`,
/// pod-security-admission/api/constants.go:35,38).
pub fn record_pod_security_audit(outcome: &PodSecurityOutcome) {
    let kvs: Vec<(String, String)> = outcome
        .audit_annotations
        .iter()
        .map(|(k, v)| (format!("pod-security.kubernetes.io/{k}"), v.clone()))
        .collect();
    if !kvs.is_empty() {
        crate::audit::add_audit_annotations(&kvs);
    }
}

/// The check registry for the default checks, built once
/// (`policy.NewEvaluator(policy.DefaultChecks(), emulationVersion)`,
/// admission.go `CompleteConfiguration`). The emulation version is the
/// binary's 1.35, which is not older than the newest check (1.35), so it
/// does not lower the cached maximum.
pub fn pod_security_registry() -> &'static pod_security_policy::CheckRegistry {
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
