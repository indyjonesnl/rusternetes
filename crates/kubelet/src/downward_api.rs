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
use rusternetes_common::validation::metav1::is_qualified_name;

/// Error returned when a DownwardAPI field path or resource selector is
/// unsupported or cannot be resolved against the supplied pod.
#[derive(Debug, PartialEq, Eq)]
pub enum DownwardError {
    /// The supplied `fieldRef.fieldPath` is not one the kubelet supports
    /// (e.g. `spec.unknownField`).
    UnsupportedField(String),
    /// A subscripted `fieldRef.fieldPath` that `ExtractFieldPathAsString`
    /// rejects; carries the verbatim upstream message.
    FieldPath(String),
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
            Self::UnsupportedField(p) => write!(f, "unsupported fieldPath: {p}"),
            Self::FieldPath(m) => f.write_str(m),
            Self::UnsupportedResource(r) => write!(f, "Unsupported resource field: {r}"),
            Self::MissingSpec => f.write_str("Pod has no spec"),
            Self::ContainerNotFound(n) => write!(f, "Container {n} not found"),
            Self::NoContainers => f.write_str("Pod has no containers"),
        }
    }
}

impl std::error::Error for DownwardError {}

/// Resolve a downwardAPI/projected **volume** `fieldRef.fieldPath` against the
/// pod. Port of `fieldpath.ExtractFieldPathAsString`
/// (`pkg/fieldpath/fieldpath.go:55-90`), the only resolver the volume side
/// reaches (`pkg/volume/downwardapi/downwardapi.go:85`):
///
/// - `metadata.name`, `metadata.namespace`, `metadata.uid`
/// - `metadata.labels`, `metadata.annotations` (`FormatMap`: `key="value"`
///   lines joined by `\n`, sorted by key, no trailing newline)
/// - `metadata.labels['key']`, `metadata.annotations['key']` (single value)
///
/// Everything else, including `status.*` / `spec.*`, is an error
/// (`unsupported fieldPath: %v`). Those env-only paths are resolved by
/// `podFieldSelectorRuntimeValue` (`kubelet_pods.go:1000-1019`), ported in
/// `cri_runtime`; volume validation (`validVolumeDownwardAPIFieldPathExpressions`,
/// `pkg/apis/core/validation/validation.go:1114`) rejects them up front.
pub fn resolve_pod_field(pod: &Pod, field_path: &str) -> Result<String, DownwardError> {
    if let Some((path, subscript)) = split_maybe_subscripted_path(field_path) {
        let invalid = |errs: Vec<String>| {
            DownwardError::FieldPath(format!(
                "invalid key subscript in {field_path}: {}",
                errs.join(";")
            ))
        };
        return match path {
            "metadata.annotations" => {
                let errs = is_qualified_name(&subscript.to_lowercase());
                if !errs.is_empty() {
                    return Err(invalid(errs));
                }
                Ok(pod
                    .metadata
                    .annotations
                    .as_ref()
                    .and_then(|m| m.get(subscript))
                    .cloned()
                    .unwrap_or_default())
            }
            "metadata.labels" => {
                let errs = is_qualified_name(subscript);
                if !errs.is_empty() {
                    return Err(invalid(errs));
                }
                Ok(pod
                    .metadata
                    .labels
                    .as_ref()
                    .and_then(|m| m.get(subscript))
                    .cloned()
                    .unwrap_or_default())
            }
            _ => Err(DownwardError::FieldPath(format!(
                "fieldPath \"{field_path}\" does not support subscript"
            ))),
        };
    }
    match field_path {
        "metadata.annotations" => Ok(render_kv_map(pod.metadata.annotations.as_ref())),
        "metadata.labels" => Ok(render_kv_map(pod.metadata.labels.as_ref())),
        "metadata.name" => Ok(pod.metadata.name.clone()),
        "metadata.namespace" => Ok(pod.metadata.namespace.clone().unwrap_or_default()),
        "metadata.uid" => Ok(pod.metadata.uid.clone()),
        other => Err(DownwardError::UnsupportedField(other.to_string())),
    }
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

/// Default a missing request from the matching limit, on a container the caller
/// already owns a copy of.
///
/// Delegates to [`rusternetes_common::defaults::default_container_requests_from_limits`],
/// the port of `SetDefaults_Pod` (`pkg/apis/core/v1/defaults.go:169-179`), which
/// the api-server's pod-create path and the static-pod decoder now both apply —
/// so in a healthy cluster this is a no-op and every caller here sees a pod that
/// was already defaulted.
///
/// It is kept, rather than deleted, because upstream's guarantee is "every pod
/// was decoded through the scheme", and the kubelet cannot verify that for a pod
/// it reads back out of storage — one written before this defaulting existed, or
/// by any writer that bypasses the api-server, still arrives undefaulted. Being
/// idempotent, re-applying it costs nothing and keeps a `resourceFieldRef` from
/// reporting `0` for a limits-only container.
///
/// [`rusternetes_common::qos::compute_pod_qos`] compensates the same way, for
/// the same reason — its upstream, `ComputePodQOS`, likewise assumes admission
/// already ran.
pub(crate) fn default_requests_from_limits(
    container: &mut rusternetes_common::resources::Container,
) {
    rusternetes_common::defaults::default_container_requests_from_limits(container);
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
    // Port of `fieldpath.FormatMap`
    // (`staging/src/k8s.io/kubectl/pkg/util/fieldpath/fieldpath.go:31-43`):
    // `fmtStr += fmt.Sprintf("%v=%q\n", key, m[key])` per sorted key, then
    // `strings.TrimSuffix(fmtStr, "\n")` — so NO trailing newline.
    pairs
        .iter()
        .map(|(k, v)| format!("{k}={}", go_quote(v)))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Go's `%q` (`strconv.Quote`) for a string: escapes `\\`, `"`, the C
/// control escapes, and other non-printable ASCII as `\xNN`; printable
/// Unicode passes through.
fn go_quote(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\x07' => out.push_str("\\a"),
            '\x08' => out.push_str("\\b"),
            '\x0c' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\x0b' => out.push_str("\\v"),
            c if (c as u32) < 0x20 || c as u32 == 0x7f => {
                out.push_str(&format!("\\x{:02x}", c as u32))
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// Port of `fieldpath.SplitMaybeSubscriptedPath`
/// (`pkg/fieldpath/fieldpath.go:108-120`): only the `path['key']` form is a
/// subscript; the path part must be non-empty.
fn split_maybe_subscripted_path(field_path: &str) -> Option<(&str, &str)> {
    let s = field_path.strip_suffix("']")?;
    let (path, subscript) = s.split_once("['")?;
    if path.is_empty() {
        return None;
    }
    Some((path, subscript))
}
