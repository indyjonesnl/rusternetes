//! DownwardAPI field/resource resolution — the single implementation.
//!
//! This module owns the pure logic that maps a Kubernetes DownwardAPI
//! `fieldRef.fieldPath` or `resourceFieldRef` to its rendered string value.
//! It is pure so the invariants pinned by upstream conformance tests
//! (`test/e2e/common/node/downwardapi.go` and
//! `test/e2e/common/storage/downwardapi_volume.go`) can be verified without a
//! live container runtime.
//!
//! Kubernetes exposes the downward API through two surfaces — container **env
//! vars** and downwardAPI/projected **volume files** — and upstream funnels both
//! into one pair of helpers, so a given selector renders identically either way.
//! Both kubelet surfaces here do the same: [`crate::cri_runtime::translate`]
//! (env) and [`crate::volumes::VolumeManager`] (files) are thin adapters over
//! [`resolve_pod_field`], [`resolve_container_resource`] and
//! [`resolve_container_resource_in`].
//!
//! That was not always true. Each surface used to carry its own hand-rolled
//! copy, and this module — the only one with tests — was declared in `lib.rs`
//! but *not* in `main.rs`, so the shipped kubelet binary never compiled it. The
//! copies had drifted: one floored byte quantities where upstream ceils, the
//! other defaulted unset limits to a hardcoded 4 cores / 8 GiB instead of the
//! node's allocatable, and neither searched init containers.
//!
//! K8s references:
//!   - `pkg/kubelet/kubelet_pods.go::podFieldSelectorRuntimeValue`
//!   - `pkg/kubelet/kubelet_pods.go::containerResourceRuntimeValue`
//!   - `pkg/kubelet/kubelet_resources.go::defaultPodLimitsForDownwardAPI`
//!   - `pkg/api/v1/resource/helpers.go::ExtractContainerResourceValue`
//!   - `pkg/volume/downwardapi/downwardapi.go` (volume side)

use rusternetes_common::quantity::Quantity;
use rusternetes_common::resources::{Pod, ResourceFieldSelector};

/// Error returned when a DownwardAPI field path or resource selector is
/// unsupported or cannot be resolved against the supplied pod.
#[derive(Debug, PartialEq, Eq)]
pub enum DownwardError {
    /// The supplied `fieldRef.fieldPath` is not one the kubelet supports
    /// (e.g. `spec.unknownField`).
    UnsupportedField(String),
    /// The supplied `resourceFieldRef.resource` is not one the kubelet
    /// supports (e.g. `limits.unknown`).
    UnsupportedResource(String),
    /// The pod has no `spec` (degenerate object — should never reach the
    /// kubelet, but defensive in tests).
    MissingSpec,
    /// `resourceFieldRef.containerName` was set but no such container
    /// exists in the pod spec.
    ContainerNotFound(String),
    /// `resourceFieldRef.containerName` was unset and the pod has no
    /// containers (degenerate).
    NoContainers,
}

impl std::fmt::Display for DownwardError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedField(p) => write!(f, "Unsupported field path: {p}"),
            Self::UnsupportedResource(r) => write!(f, "Unsupported resource field: {r}"),
            Self::MissingSpec => f.write_str("Pod has no spec"),
            Self::ContainerNotFound(n) => write!(f, "Container {n} not found"),
            Self::NoContainers => f.write_str("Pod has no containers"),
        }
    }
}

impl std::error::Error for DownwardError {}

/// Resolve a DownwardAPI `fieldRef.fieldPath` against the pod.
///
/// Supported paths (mirrors upstream
/// `pkg/kubelet/kubelet_pods.go::podFieldSelectorRuntimeValue`):
///
/// - `metadata.name`, `metadata.namespace`, `metadata.uid`
/// - `metadata.labels`, `metadata.annotations` (rendered as
///   `key="value"\n` lines, sorted by key)
/// - `metadata.labels['key']`, `metadata.annotations['key']` (single value)
/// - `spec.nodeName`, `spec.serviceAccountName`
/// - `status.podIP`, `status.hostIP`
pub fn resolve_pod_field(pod: &Pod, field_path: &str) -> Result<String, DownwardError> {
    let value = match field_path {
        "metadata.name" => pod.metadata.name.clone(),
        "metadata.namespace" => pod
            .metadata
            .namespace
            .clone()
            .unwrap_or_else(|| "default".to_string()),
        "metadata.uid" => pod.metadata.uid.clone(),
        "spec.nodeName" => pod
            .spec
            .as_ref()
            .and_then(|s| s.node_name.clone())
            .unwrap_or_default(),
        "spec.serviceAccountName" => pod
            .spec
            .as_ref()
            .and_then(|s| s.service_account_name.clone())
            .unwrap_or_else(|| "default".to_string()),
        "status.podIP" => pod
            .status
            .as_ref()
            .and_then(|s| s.pod_ip.clone())
            .unwrap_or_default(),
        "status.hostIP" => pod
            .status
            .as_ref()
            .and_then(|s| s.host_ip.clone())
            .unwrap_or_else(|| "127.0.0.1".to_string()),
        "metadata.labels" => render_kv_map(pod.metadata.labels.as_ref()),
        "metadata.annotations" => render_kv_map(pod.metadata.annotations.as_ref()),
        other => {
            if let Some(key) = strip_bracket_key(other, "metadata.labels[") {
                pod.metadata
                    .labels
                    .as_ref()
                    .and_then(|m| m.get(key))
                    .cloned()
                    .unwrap_or_default()
            } else if let Some(key) = strip_bracket_key(other, "metadata.annotations[") {
                pod.metadata
                    .annotations
                    .as_ref()
                    .and_then(|m| m.get(key))
                    .cloned()
                    .unwrap_or_default()
            } else {
                return Err(DownwardError::UnsupportedField(other.to_string()));
            }
        }
    };
    Ok(value)
}

/// Resolve a DownwardAPI `resourceFieldRef` against the pod, defaulting unset
/// limits from the node's allocatable.
///
/// Port of upstream `ExtractResourceValueByContainerNameAndNodeAllocatable`
/// (`pkg/api/v1/resource/helpers.go:88-101`), which is the single funnel *both*
/// downward-API consumers reach upstream:
///
/// - **env vars** — `pkg/kubelet/kubelet_pods.go:859-863` runs the pod through
///   `defaultPodLimitsForDownwardAPI` (`pkg/kubelet/kubelet_resources.go:38-68`,
///   which applies `MergeContainerResourceLimits` with `node.Status.Allocatable`)
///   and then calls `containerResourceRuntimeValue`;
/// - **downwardAPI / projected volumes** — `pkg/volume/downwardapi/downwardapi.go:266`
///   calls this function directly.
///
/// Both kubelet call sites in this crate ([`crate::cri_runtime::translate`] for
/// env vars, [`crate::volumes::VolumeManager`] for volume files) go through here
/// so the same `resourceFieldRef` cannot render two different values depending
/// on which one the pod happened to use.
///
/// `node_allocatable` is the node's `status.allocatable`; `None` means "not
/// known here" and skips the merge, matching a node object the kubelet has not
/// registered yet.
pub fn resolve_container_resource(
    pod: &Pod,
    sel: &ResourceFieldSelector,
    node_allocatable: Option<&std::collections::HashMap<String, String>>,
) -> Result<String, DownwardError> {
    let spec = pod.spec.as_ref().ok_or(DownwardError::MissingSpec)?;

    let container = match sel.container_name.as_deref() {
        Some(name) => find_container_in_pod(spec, name)
            .ok_or_else(|| DownwardError::ContainerNotFound(name.to_string()))?,
        None => spec.containers.first().ok_or(DownwardError::NoContainers)?,
    };

    resolve_container_resource_in(container, sel, node_allocatable)
}

/// As [`resolve_container_resource`], but against a container the caller already
/// holds instead of looking one up by `sel.container_name`.
///
/// This is the env-var entry point. Upstream `containerResourceRuntimeValue`
/// (`pkg/kubelet/kubelet_pods.go:1026-1033`) uses the container it was handed
/// whenever `fs.ContainerName` is empty — and for an **env** `resourceFieldRef`
/// it is always empty, because `validateContainerResourceFieldSelector`
/// (`pkg/apis/core/validation/validation.go`) rejects a non-empty
/// `containerName` outside a volume with `"not supported"`. (The mirror-image
/// rule makes `containerName` *required* for a downwardAPI volume, which is why
/// the pod-based entry point above is the volume one.)
pub fn resolve_container_resource_in(
    container: &rusternetes_common::resources::Container,
    sel: &ResourceFieldSelector,
    node_allocatable: Option<&std::collections::HashMap<String, String>>,
) -> Result<String, DownwardError> {
    // Upstream defaults a `DeepCopy`, never the caller's container.
    let mut container = container.clone();
    default_requests_from_limits(&mut container);
    if let Some(allocatable) = node_allocatable {
        merge_container_resource_limits(&mut container, allocatable);
    }

    extract_container_resource_value(sel, &container)
}

/// Find a container by name. Port of upstream `findContainerInPod`
/// (`pkg/api/v1/resource/helpers.go:172-185`): regular containers first, then
/// **init containers** — a `resourceFieldRef.containerName` naming a sidecar or
/// init container resolves upstream and must resolve here.
fn find_container_in_pod<'a>(
    spec: &'a rusternetes_common::resources::PodSpec,
    name: &str,
) -> Option<&'a rusternetes_common::resources::Container> {
    spec.containers
        .iter()
        .chain(spec.init_containers.iter().flatten())
        .find(|c| c.name == name)
}

/// Default a missing request from the matching limit.
///
/// Shared with [`crate::eviction::get_qos_class`], which needs the same
/// defaulting for the same reason: upstream's `ComputePodQOS` also assumes it
/// already happened.
///
/// Upstream does this in the **api-server**, not the kubelet:
/// `SetDefaults_Pod` (`pkg/apis/core/v1/defaults.go:164-180`) copies every
/// `limits` entry into `requests` when the request is absent, so by the time a
/// pod reaches the kubelet `requests.cpu` is already populated. The rusternetes
/// api-server does not implement that defaulting yet (`handlers/defaults.rs`
/// has no resource pass), so a limits-only pod would otherwise report
/// `requests.memory` as `0` here while upstream reports the limit. Applied to
/// the local copy only; tracked for a proper api-server-side port.
pub(crate) fn default_requests_from_limits(
    container: &mut rusternetes_common::resources::Container,
) {
    let Some(resources) = container.resources.as_mut() else {
        return;
    };
    let Some(limits) = resources.limits.clone() else {
        return;
    };
    let requests = resources.requests.get_or_insert_with(Default::default);
    for (key, value) in limits {
        requests.entry(key).or_insert(value);
    }
}

/// Fill an unset (or zero) cpu / memory / ephemeral-storage **limit** from the
/// node's allocatable. Port of upstream `MergeContainerResourceLimits`
/// (`pkg/api/v1/resource/helpers.go:189-203`), including its comment that
/// `hugepages-*` is deliberately excluded because hugepages are never
/// overcommitted and therefore always carry an explicit limit.
///
/// Note this touches limits **only** — upstream leaves requests alone, so an
/// unset `requests.memory` reports `0`, not the node's allocatable.
fn merge_container_resource_limits(
    container: &mut rusternetes_common::resources::Container,
    allocatable: &std::collections::HashMap<String, String>,
) {
    let resources =
        container
            .resources
            .get_or_insert(rusternetes_common::types::ResourceRequirements {
                limits: None,
                requests: None,
                claims: None,
            });
    let limits = resources.limits.get_or_insert_with(Default::default);
    for name in ["cpu", "memory", "ephemeral-storage"] {
        let unset_or_zero = match limits.get(name) {
            None => true,
            Some(raw) => Quantity::parse(raw.trim())
                .map(|q| q.is_zero())
                .unwrap_or(true),
        };
        if unset_or_zero {
            if let Some(cap) = allocatable.get(name) {
                limits.insert(name.to_string(), cap.clone());
            }
        }
    }
}

/// Render one `resourceFieldRef` against an already-defaulted container.
///
/// Port of upstream `ExtractContainerResourceValue`
/// (`pkg/api/v1/resource/helpers.go:105-142`):
///
/// - an unset or literal-zero `divisor` means `1` (helpers.go:106-111 — the Go
///   client marshals an unset `resource.Quantity` as `"0"`, so that is what
///   arrives on the wire for every field left at its default);
/// - cpu divides **milli**-values, everything else divides base values
///   (`convertResourceCPUToString` uses `MilliValue()`, the other three use
///   `Value()`, helpers.go:146-170);
/// - **every** conversion rounds **up** — all four `convertResource*ToString`
///   helpers wrap the division in `math.Ceil`;
/// - a resource that is not cpu / memory / ephemeral-storage / `hugepages-*`
///   is an error (helpers.go:141), not a passthrough of the raw string;
/// - a quantity that is not set reads as the zero `Quantity`, i.e. `"0"` —
///   Go's `Limits.Cpu()` returns a zero quantity for a missing key.
fn extract_container_resource_value(
    sel: &ResourceFieldSelector,
    container: &rusternetes_common::resources::Container,
) -> Result<String, DownwardError> {
    let unsupported = || DownwardError::UnsupportedResource(sel.resource.clone());
    let (kind, name) = sel.resource.split_once('.').ok_or_else(unsupported)?;

    let is_cpu = name == "cpu";
    if !is_cpu
        && name != "memory"
        && name != "ephemeral-storage"
        && !is_huge_page_resource_name(name)
    {
        return Err(unsupported());
    }

    let list = match kind {
        "limits" => container.resources.as_ref().and_then(|r| r.limits.as_ref()),
        "requests" => container
            .resources
            .as_ref()
            .and_then(|r| r.requests.as_ref()),
        _ => return Err(unsupported()),
    };

    // A missing key is the zero Quantity upstream, not an omitted value.
    let value = match list.and_then(|m| m.get(name)) {
        Some(raw) => scaled_value(raw, is_cpu).ok_or_else(unsupported)?,
        None => 0,
    };
    // `divisor.Cmp(zeroQuantity) == 0` → `resource.MustParse("1")`. Note the
    // substitute is a *Quantity*, so for cpu it contributes `MilliValue()` ==
    // 1000, which is why the natural cpu unit is a whole core and not a milli.
    let divisor = match sel.divisor.as_deref() {
        None | Some("") => scaled_value("1", is_cpu),
        Some(raw) => scaled_value(raw, is_cpu),
    };
    let divisor = match divisor {
        None | Some(0) => scaled_value("1", is_cpu).unwrap_or(1),
        Some(d) => d,
    };

    Ok(ceil_div(value, divisor).to_string())
}

/// Port of upstream `IsHugePageResourceName`
/// (`pkg/api/v1/resource/helpers.go:207-209`) for the bare resource name (the
/// `requests.`/`limits.` prefix is already split off by the caller).
fn is_huge_page_resource_name(name: &str) -> bool {
    name.starts_with("hugepages-")
}

/// The numeric value a quantity contributes to the division: milli-units for
/// cpu (`Quantity.MilliValue()`), base units otherwise (`Quantity.Value()`).
fn scaled_value(raw: &str, is_cpu: bool) -> Option<i128> {
    let q = Quantity::parse(raw.trim()).ok()?;
    Some(if is_cpu { q.milli_value() } else { q.value() })
}

/// Ceiling division, the integer equivalent of upstream's
/// `int64(math.Ceil(float64(value) / float64(divisor)))`. Kept in `i128` so a
/// near-`i64::MAX` byte count plus the divisor cannot overflow.
fn ceil_div(numerator: i128, denominator: i128) -> i128 {
    if denominator == 0 {
        return 0;
    }
    let (q, r) = (numerator / denominator, numerator % denominator);
    // Round away from zero only when the remainder shares the quotient's sign,
    // i.e. the true result is positive and fractional.
    if r != 0 && (r > 0) == (denominator > 0) {
        q + 1
    } else {
        q
    }
}

fn render_kv_map(map: Option<&std::collections::HashMap<String, String>>) -> String {
    let Some(map) = map else {
        return String::new();
    };
    let mut pairs: Vec<_> = map.iter().collect();
    pairs.sort_by(|a, b| a.0.cmp(b.0));
    let mut out = pairs
        .iter()
        .map(|(k, v)| format!("{k}=\"{v}\""))
        .collect::<Vec<_>>()
        .join("\n");
    if !out.is_empty() {
        out.push('\n');
    }
    out
}

/// Strip a `prefix[` and trailing `']` to recover the bracketed key in
/// `metadata.labels['key']` / `metadata.annotations['key']`. Returns
/// `None` when the path does not match.
fn strip_bracket_key<'a>(path: &'a str, prefix: &str) -> Option<&'a str> {
    let rest = path.strip_prefix(prefix)?;
    let inner = rest.strip_suffix(']')?;
    // K8s allows both single and double quotes, plus unquoted keys.
    let unquoted = inner
        .strip_prefix('\'')
        .and_then(|s| s.strip_suffix('\''))
        .or_else(|| inner.strip_prefix('"').and_then(|s| s.strip_suffix('"')))
        .unwrap_or(inner);
    Some(unquoted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::{Container, Pod, PodSpec, PodStatus};
    use rusternetes_common::types::{ObjectMeta, ResourceRequirements, TypeMeta};
    use std::collections::HashMap;

    fn make_pod() -> Pod {
        Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("p").with_namespace("ns"),
            spec: Some(PodSpec {
                containers: vec![Container {
                    name: "c".to_string(),
                    image: "x".to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            status: None,
        }
    }

    #[test]
    fn metadata_name_resolves() {
        let p = make_pod();
        assert_eq!(resolve_pod_field(&p, "metadata.name").unwrap(), "p");
    }

    #[test]
    fn metadata_namespace_defaults_to_default_when_missing() {
        let mut p = make_pod();
        p.metadata.namespace = None;
        assert_eq!(
            resolve_pod_field(&p, "metadata.namespace").unwrap(),
            "default"
        );
    }

    #[test]
    fn unknown_field_returns_unsupported_error() {
        let p = make_pod();
        let err = resolve_pod_field(&p, "spec.unknownField").unwrap_err();
        assert!(matches!(err, DownwardError::UnsupportedField(_)));
    }

    #[test]
    fn status_host_ip_defaults_to_loopback() {
        let p = make_pod();
        assert_eq!(resolve_pod_field(&p, "status.hostIP").unwrap(), "127.0.0.1");
    }

    #[test]
    fn labels_subscript_resolves_value() {
        let mut p = make_pod();
        let mut labels = HashMap::new();
        labels.insert("app".to_string(), "web".to_string());
        p.metadata.labels = Some(labels);
        assert_eq!(
            resolve_pod_field(&p, "metadata.labels['app']").unwrap(),
            "web"
        );
    }

    /// The node's advertised `status.allocatable`, as
    /// `kubelet::node_allocatable_map` builds it.
    fn allocatable() -> HashMap<String, String> {
        HashMap::from([
            ("cpu".to_string(), "4".to_string()),
            ("memory".to_string(), "8Gi".to_string()),
            ("ephemeral-storage".to_string(), "100Gi".to_string()),
        ])
    }

    fn sel(resource: &str, divisor: Option<&str>) -> ResourceFieldSelector {
        ResourceFieldSelector {
            container_name: Some("c".into()),
            resource: resource.into(),
            divisor: divisor.map(str::to_string),
        }
    }

    fn with_resources(pod: &mut Pod, limits: &[(&str, &str)], requests: &[(&str, &str)]) {
        let to_map = |kv: &[(&str, &str)]| -> Option<HashMap<String, String>> {
            if kv.is_empty() {
                None
            } else {
                Some(
                    kv.iter()
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect(),
                )
            }
        };
        if let Some(ref mut spec) = pod.spec {
            spec.containers[0].resources = Some(ResourceRequirements {
                limits: to_map(limits),
                requests: to_map(requests),
                claims: None,
            });
        }
    }

    #[test]
    fn limits_cpu_default_to_node_allocatable() {
        let p = make_pod();
        // 4000 millicores ÷ 1000 (cores divisor) = 4
        assert_eq!(
            resolve_container_resource(&p, &sel("limits.cpu", None), Some(&allocatable())).unwrap(),
            "4"
        );
    }

    /// `limits.ephemeral-storage` defaults from the node's allocatable like any
    /// other mergeable limit (`MergeContainerResourceLimits`,
    /// `pkg/api/v1/resource/helpers.go:196`), which lists cpu, memory **and**
    /// ephemeral-storage.
    ///
    /// The volume-side copy of this logic hardcoded an 8 GiB fallback for every
    /// byte-valued resource, so a downwardAPI *volume* file for
    /// `limits.ephemeral-storage` on a container with no explicit limit wrote
    /// `8589934592` while the *env var* for the very same selector — resolved by
    /// the other copy, which did consult the node — wrote `107374182400`.
    #[test]
    fn limits_ephemeral_storage_defaults_to_node_allocatable_not_8gi() {
        let p = make_pod();
        assert_eq!(
            resolve_container_resource(
                &p,
                &sel("limits.ephemeral-storage", None),
                Some(&allocatable())
            )
            .unwrap(),
            (100i64 * 1024 * 1024 * 1024).to_string()
        );
    }

    /// Every `convertResource*ToString` helper rounds **up**
    /// (`math.Ceil`, `pkg/api/v1/resource/helpers.go:146-170`). The env-var copy
    /// of this logic floored byte quantities (`bytes / div_bytes`), so a limit
    /// that is not a whole multiple of the divisor reported one unit short.
    ///
    /// 100M = 100_000_000 bytes; 100_000_000 / 1Mi = 95.367…, so upstream says
    /// 96 and the flooring copy said 95.
    #[test]
    fn memory_divisor_rounds_up_not_down() {
        let mut p = make_pod();
        with_resources(&mut p, &[("memory", "100M")], &[]);
        assert_eq!(
            resolve_container_resource(
                &p,
                &sel("limits.memory", Some("1Mi")),
                Some(&allocatable())
            )
            .unwrap(),
            "96"
        );
    }

    /// `findContainerInPod` (`pkg/api/v1/resource/helpers.go:172-185`) searches
    /// `Spec.Containers` **and then** `Spec.InitContainers`. Naming a sidecar or
    /// init container in `resourceFieldRef.containerName` resolves upstream; both
    /// kubelet copies searched only `spec.containers` and failed the pod.
    #[test]
    fn container_name_resolves_an_init_container() {
        let mut p = make_pod();
        if let Some(ref mut spec) = p.spec {
            spec.init_containers = Some(vec![Container {
                name: "init".to_string(),
                image: "x".to_string(),
                resources: Some(ResourceRequirements {
                    limits: Some(HashMap::from([("memory".to_string(), "64Mi".to_string())])),
                    requests: None,
                    claims: None,
                }),
                ..Default::default()
            }]);
        }
        let mut s = sel("limits.memory", None);
        s.container_name = Some("init".into());
        assert_eq!(
            resolve_container_resource(&p, &s, Some(&allocatable())).unwrap(),
            (64 * 1024 * 1024).to_string()
        );
    }

    /// `SetDefaults_Pod` (`pkg/apis/core/v1/defaults.go:164-180`) copies limits
    /// into unset requests at admission, so `requests.memory` on a limits-only
    /// container reports the limit. The volume-side copy had no such fallback and
    /// reported its hardcoded 8 GiB instead.
    #[test]
    fn unset_request_defaults_to_the_limit() {
        let mut p = make_pod();
        with_resources(&mut p, &[("memory", "64Mi")], &[]);
        assert_eq!(
            resolve_container_resource(&p, &sel("requests.memory", None), Some(&allocatable()))
                .unwrap(),
            (64 * 1024 * 1024).to_string()
        );
    }

    /// `MergeContainerResourceLimits` fills **limits** only (helpers.go:191-202).
    /// With neither a request nor a limit set, `Requests.Memory()` is the zero
    /// Quantity, so upstream renders `"0"` — not the node's allocatable.
    #[test]
    fn unset_request_without_a_limit_is_zero_not_allocatable() {
        let p = make_pod();
        assert_eq!(
            resolve_container_resource(&p, &sel("requests.memory", None), Some(&allocatable()))
                .unwrap(),
            "0"
        );
    }

    /// An unset `resource.Quantity` marshals as `"0"` on the wire, and
    /// helpers.go:106-111 maps that to a divisor of 1. `None`, `""` and `"0"`
    /// must therefore all mean "natural unit".
    #[test]
    fn zero_empty_and_absent_divisors_all_mean_one() {
        let mut p = make_pod();
        with_resources(&mut p, &[("memory", "64Mi")], &[]);
        let bytes = (64 * 1024 * 1024).to_string();
        for divisor in [None, Some(""), Some("0")] {
            assert_eq!(
                resolve_container_resource(
                    &p,
                    &sel("limits.memory", divisor),
                    Some(&allocatable())
                )
                .unwrap(),
                bytes,
                "divisor {divisor:?}"
            );
        }
    }

    /// helpers.go:129-141 accepts `hugepages-<size>` under either prefix and
    /// errors on anything else — it does not pass the raw string through.
    #[test]
    fn hugepages_divide_as_bytes_and_unknown_resources_error() {
        let mut p = make_pod();
        with_resources(&mut p, &[("hugepages-2Mi", "4Mi")], &[]);
        assert_eq!(
            resolve_container_resource(
                &p,
                &sel("limits.hugepages-2Mi", Some("1Mi")),
                Some(&allocatable())
            )
            .unwrap(),
            "4"
        );
        assert_eq!(
            resolve_container_resource(&p, &sel("limits.nvidia.com/gpu", None), None).unwrap_err(),
            DownwardError::UnsupportedResource("limits.nvidia.com/gpu".to_string())
        );
    }

    #[test]
    fn limits_memory_explicit_returns_bytes() {
        let mut p = make_pod();
        let mut limits = HashMap::new();
        limits.insert("memory".to_string(), "64Mi".to_string());
        if let Some(ref mut spec) = p.spec {
            spec.containers[0].resources = Some(ResourceRequirements {
                limits: Some(limits),
                requests: None,
                claims: None,
            });
        }
        // 64 MiB = 67_108_864 bytes
        assert_eq!(
            resolve_container_resource(&p, &sel("limits.memory", None), Some(&allocatable()))
                .unwrap(),
            (64 * 1024 * 1024).to_string()
        );
    }

    #[test]
    fn pod_status_pod_ip_round_trips() {
        let mut p = make_pod();
        p.status = Some(PodStatus {
            pod_ip: Some("10.244.0.5".into()),
            ..Default::default()
        });
        assert_eq!(resolve_pod_field(&p, "status.podIP").unwrap(), "10.244.0.5");
    }
}
