//! Map CRI container status onto rusternetes [`ContainerStatus`].
//!
//! Pure translation in the other direction from [`super::translate`]: the
//! runtime reports a `runtime.v1::ContainerStatus`, and the kubelet's pod-status
//! machinery wants a rusternetes `ContainerStatus`. Readiness/started here are
//! derived purely from the runtime state; the kubelet overlays probe results on
//! top.

use rusternetes_common::quantity::{Format, Quantity};
use rusternetes_common::resources::pod::{
    Container, ContainerState, ContainerStateRunning, ContainerStateTerminated,
    ContainerStateWaiting, ContainerStatus,
};
use rusternetes_common::types::ResourceRequirements;
use rusternetes_cri::v1;
use std::collections::HashMap;

/// The CFS constants live next to the forward conversion in [`super::translate`]
/// (`milli_cpu_to_shares` / `milli_cpu_to_quota`); this module is the inverse
/// and must not grow a second copy of them.
use super::translate::{MILLI_CPU_TO_CPU, MIN_MILLI_CPU_LIMIT, MIN_SHARES, SHARES_PER_CPU};

/// Convert `cpu.shares` to milli-CPU. Port of upstream `sharesToMilliCPU`
/// (pkg/kubelet/cm/helpers_linux.go:383-389).
fn shares_to_milli_cpu(shares: i64) -> i64 {
    if shares < MIN_SHARES {
        return 0;
    }
    // ceil(shares * 1000 / 1024), integer-only to match Go's math.Ceil on the
    // float division without importing floating point. Both operands are
    // positive here, so the plain round-up form is exact.
    let numerator = shares * MILLI_CPU_TO_CPU;
    (numerator + SHARES_PER_CPU - 1) / SHARES_PER_CPU
}

/// Convert `cpu.cfs_quota_us`/`cpu.cfs_period_us` to milli-CPU. Port of
/// upstream `quotaToMilliCPU` (pkg/kubelet/cm/helpers_linux.go:394-399).
fn quota_to_milli_cpu(quota: i64, period: i64) -> i64 {
    if quota == -1 || period == 0 {
        return 0;
    }
    (quota * MILLI_CPU_TO_CPU) / period
}

/// The milli-CPU value of a quantity string, or `None` if it does not parse.
fn milli_cpu_of(raw: &str) -> Option<i64> {
    Quantity::parse(raw).ok().map(|q| q.milli_value() as i64)
}

/// Resources the runtime actually applied, as reported over CRI. Port of the
/// linux half of upstream `toKubeContainerResources`
/// (pkg/kubelet/kuberuntime/kuberuntime_container_linux.go:384-412).
///
/// Returns `(cpu_request, cpu_limit, memory_limit)` in milli-CPU / milli-CPU /
/// bytes, each `None` when the runtime did not report it.
fn cri_applied_resources(cri: &v1::ContainerStatus) -> (Option<i64>, Option<i64>, Option<i64>) {
    let Some(linux) = cri.resources.as_ref().and_then(|r| r.linux.as_ref()) else {
        return (None, None, None);
    };

    let cpu_limit = if linux.cpu_period > 0 {
        let milli = quota_to_milli_cpu(linux.cpu_quota, linux.cpu_period);
        (milli > 0).then_some(milli)
    } else {
        None
    };
    let cpu_request = if linux.cpu_shares > 0 {
        let milli = shares_to_milli_cpu(linux.cpu_shares);
        (milli > 0).then_some(milli)
    } else {
        None
    };
    let memory_limit = (linux.memory_limit_in_bytes > 0).then_some(linux.memory_limit_in_bytes);

    (cpu_request, cpu_limit, memory_limit)
}

/// `status.resources` for one container. Port of upstream's
/// `convertContainerStatusResources` closure
/// (pkg/kubelet/kubelet_pods.go:2338-2404).
///
/// The reported value starts from the *allocated* spec resources and is
/// overridden, per resource, by what the runtime actually applied — so during
/// an in-place resize the status reflects the cgroup reality, not the desired
/// state. Non-resizable resources keep their allocated value because the CRI
/// status has nothing to say about them.
///
/// Divergence from upstream, deliberate: upstream's `preserveOldResourcesValue`
/// carries the *previously reported* value forward when the runtime omits a
/// resource for a still-running container with an unchanged container ID. We
/// fall back to the allocated value instead — `old` is accepted and used for
/// the same running/same-ID guard, but only for resources the previous status
/// actually carried. See the callers in `runtime.rs`.
fn container_status_resources(
    allocated: &Container,
    cri: &v1::ContainerStatus,
    old: Option<&ContainerStatus>,
) -> Option<ResourceRequirements> {
    let allocated_resources = allocated.resources.as_ref()?;

    // "If the container isn't running, just use the allocated resources."
    // (kubelet_pods.go:2357-2359)
    let running = cri.state == v1::ContainerState::ContainerRunning as i32;
    if !running {
        return Some(allocated_resources.clone());
    }

    let (cpu_request, cpu_limit, memory_limit) = cri_applied_resources(cri);

    // Upstream only preserves an old value for a running container whose
    // container ID has not changed (kubelet_pods.go:2345-2355).
    let old_resources = old
        .filter(|o| {
            matches!(
                o.state,
                Some(ContainerState {
                    running: Some(ContainerStateRunning { .. }),
                    ..
                })
            ) && o.container_id.as_deref() == Some(cri.id.as_str())
        })
        .and_then(|o| o.resources.as_ref());

    let mut resources = allocated_resources.clone();

    if let Some(limits) = resources.limits.as_mut() {
        match cpu_limit {
            // "If both the allocated & actual resources are at or below the
            // minimum effective limit, preserve the allocated value in the API
            // to avoid confusion and simplify comparisons." (:2369-2371)
            Some(milli)
                if milli > MIN_MILLI_CPU_LIMIT
                    || limits
                        .get("cpu")
                        .and_then(|c| milli_cpu_of(c))
                        .is_some_and(|a| a > MIN_MILLI_CPU_LIMIT) =>
            {
                limits.insert(
                    "cpu".to_string(),
                    Quantity::from_milli_value(milli, Format::DecimalSI).canonical_string(),
                );
            }
            _ => preserve_old(limits, "cpu", old_resources.and_then(|r| r.limits.as_ref())),
        }
        match memory_limit {
            Some(bytes) => {
                limits.insert(
                    "memory".to_string(),
                    Quantity::from_value(bytes, Format::BinarySI).canonical_string(),
                );
            }
            None => preserve_old(
                limits,
                "memory",
                old_resources.and_then(|r| r.limits.as_ref()),
            ),
        }
    }

    if let Some(requests) = resources.requests.as_mut() {
        match cpu_request {
            // Same MinShares reasoning as the limit above (:2387-2389).
            Some(milli)
                if milli > MIN_SHARES
                    || requests
                        .get("cpu")
                        .and_then(|c| milli_cpu_of(c))
                        .is_some_and(|a| a > MIN_SHARES) =>
            {
                requests.insert(
                    "cpu".to_string(),
                    Quantity::from_milli_value(milli, Format::DecimalSI).canonical_string(),
                );
            }
            _ => preserve_old(
                requests,
                "cpu",
                old_resources.and_then(|r| r.requests.as_ref()),
            ),
        }
        // Memory requests are not resizable in place and the runtime does not
        // report them, so the allocated value stands.
    }

    Some(resources)
}

/// Carry a previously-reported value forward when the runtime omitted it. Port
/// of upstream's `preserveOldResourcesValue` (kubelet_pods.go:2345-2355); the
/// running/same-ID guard is applied by the caller.
fn preserve_old(
    target: &mut HashMap<String, String>,
    name: &str,
    old: Option<&HashMap<String, String>>,
) {
    if let Some(value) = old.and_then(|o| o.get(name)) {
        target.insert(name.to_string(), value.clone());
    }
}

/// Convert a unix-nanoseconds timestamp into an RFC3339 string, or `None` for a
/// zero/absent timestamp.
pub fn nanos_to_rfc3339(nanos: i64) -> Option<String> {
    if nanos == 0 {
        return None;
    }
    let secs = nanos.div_euclid(1_000_000_000);
    let sub = nanos.rem_euclid(1_000_000_000) as u32;
    chrono::DateTime::<chrono::Utc>::from_timestamp(secs, sub).map(|dt| dt.to_rfc3339())
}

fn empty_to_none(s: &str) -> Option<String> {
    if s.is_empty() {
        None
    } else {
        Some(s.to_string())
    }
}

/// Upstream `kubecontainer.MaxContainerTerminationMessageLength` (release-1.35,
/// `pkg/kubelet/container/runtime.go`): the termination message is capped at 4
/// KiB, keeping the trailing bytes.
pub const MAX_TERMINATION_MESSAGE_LENGTH: usize = 1024 * 4;

/// Upstream `kubecontainer.MaxContainerTerminationMessageLogLength` (release-1.35,
/// `pkg/kubelet/container/runtime.go`): the log-fallback tail is capped at 2 KiB.
pub const MAX_TERMINATION_MESSAGE_LOG_LENGTH: usize = 1024 * 2;

/// Upstream `kubecontainer.MaxContainerTerminationMessageLogLines` (release-1.35,
/// `pkg/kubelet/container/runtime.go`): log-fallback reads at most 80 tail lines.
pub const MAX_TERMINATION_MESSAGE_LOG_LINES: i32 = 80;

/// Resolve a terminated container's `message` the way upstream
/// `kuberuntime_container.go::getTerminationMessage` + `convertToKubeContainerStatus`
/// does (release-1.35, `pkg/kubelet/kuberuntime/kuberuntime_container.go`).
///
/// `file_read` is `Some(contents)` when the termination-message file
/// (`terminationMessagePath`, default `/dev/termination-log`) was readable —
/// even if empty — and `None` when it was absent/unreadable. `policy` is the
/// container's `terminationMessagePolicy` (`"File"` by default, or
/// `"FallbackToLogsOnError"`). `log_tail` lazily supplies the tail of the
/// container log (already decoded from CRI framing); it is consulted only on
/// the fallback path.
///
/// Upstream contract — `getTerminationMessage` returns `(message, checkLogs)`:
/// ```go
/// // pkg/kubelet/kuberuntime/kuberuntime_container.go:583
/// return string(data), (fallbackToLogs && len(data) == 0)
/// ```
/// - file readable + non-empty → its contents win (used as-is), even on clean
///   exit. This covers `[sig-node] ... report termination message from file`.
/// - file readable + **empty** + `FallbackToLogsOnError` + non-zero exit (or
///   OOMKilled) → `checkLogs=true`, log tail is read and used as the message.
///   This is the `[sig-node] ... FallbackToLogsOnError` conformance case.
/// - file unreadable (absent) → same `checkLogs` evaluation as empty-file path.
/// - `File` policy or clean exit (exit 0, no OOMKilled) → no log fallback.
///
/// The chosen message is truncated to the last [`MAX_TERMINATION_MESSAGE_LENGTH`]
/// bytes, mirroring upstream's `tail.ReadAtMost`.
pub fn resolve_termination_message(
    file_read: Option<String>,
    policy: &str,
    exit_code: i32,
    reason: Option<&str>,
    log_tail: impl FnOnce() -> Option<String>,
) -> Option<String> {
    // `fallbackToLogs` mirrors upstream:
    // ```go
    // // pkg/kubelet/kuberuntime/kuberuntime_container.go:605
    // fallbackToLogs := annotatedInfo.TerminationMessagePolicy ==
    //     v1.TerminationMessageFallbackToLogsOnError &&
    //     cStatus.ExitCode != 0 && cStatus.Reason != "ContainerCannotRun"
    // ```
    // We also treat OOMKilled at exit 0 as an error case, matching upstream's
    // `getTerminationMessage` which passes the existing `fallbackToLogs` flag
    // unchanged — OOMKilled at exit 0 reaches this function with
    // `fallbackToLogs=true` because upstream sets it before calling here.
    let fallback =
        policy == "FallbackToLogsOnError" && (exit_code != 0 || reason == Some("OOMKilled"));

    match file_read {
        // Non-empty file always wins, regardless of policy or exit code.
        Some(ref contents) if !contents.is_empty() => non_empty(truncate_tail(contents)),
        // Empty file: upstream returns `checkLogs = (fallbackToLogs && len(data) == 0)`
        // so with FallbackToLogsOnError + error exit we fall through to the log tail.
        Some(_empty) if fallback => log_tail().and_then(|l| non_empty(truncate_tail(&l))),
        // Empty file with File policy or clean exit: no message.
        Some(_) => None,
        // File absent/unreadable: same check-logs logic as above.
        None if fallback => log_tail().and_then(|l| non_empty(truncate_tail(&l))),
        None => None,
    }
}

/// Keep the last [`MAX_TERMINATION_MESSAGE_LENGTH`] bytes of `s`, snapped to a
/// char boundary so the result stays valid UTF-8.
fn truncate_tail(s: &str) -> String {
    if s.len() <= MAX_TERMINATION_MESSAGE_LENGTH {
        return s.to_string();
    }
    let want = s.len() - MAX_TERMINATION_MESSAGE_LENGTH;
    let start = (want..s.len())
        .find(|&i| s.is_char_boundary(i))
        .unwrap_or(s.len());
    s[start..].to_string()
}

fn non_empty(s: String) -> Option<String> {
    if s.is_empty() {
        None
    } else {
        Some(s)
    }
}

/// Map the CRI runtime state into a rusternetes [`ContainerState`].
fn map_state(cri: &v1::ContainerStatus) -> ContainerState {
    match v1::ContainerState::try_from(cri.state).unwrap_or(v1::ContainerState::ContainerUnknown) {
        v1::ContainerState::ContainerCreated => ContainerState {
            waiting: Some(ContainerStateWaiting {
                reason: Some("ContainerCreating".to_string()),
                message: empty_to_none(&cri.message),
            }),
            ..Default::default()
        },
        v1::ContainerState::ContainerRunning => ContainerState {
            running: Some(ContainerStateRunning {
                started_at: nanos_to_rfc3339(cri.started_at),
            }),
            ..Default::default()
        },
        v1::ContainerState::ContainerExited => ContainerState {
            terminated: Some(ContainerStateTerminated {
                exit_code: cri.exit_code,
                signal: None,
                reason: empty_to_none(&cri.reason),
                message: empty_to_none(&cri.message),
                started_at: nanos_to_rfc3339(cri.started_at),
                finished_at: nanos_to_rfc3339(cri.finished_at),
                container_id: empty_to_none(&cri.id),
            }),
            ..Default::default()
        },
        v1::ContainerState::ContainerUnknown => ContainerState {
            waiting: Some(ContainerStateWaiting {
                reason: Some("Unknown".to_string()),
                message: empty_to_none(&cri.message),
            }),
            ..Default::default()
        },
    }
}

/// Translate a CRI [`ContainerStatus`](v1::ContainerStatus) into a rusternetes
/// one. `ready`/`started` reflect the runtime RUNNING state only; the kubelet
/// applies probe results afterwards.
///
/// `allocated` is the container's spec entry in the pod that owns it, and
/// `old` its previously-reported status, if any. Both feed
/// `status.resources`/`status.allocatedResources` the way upstream's
/// `convertToAPIContainerStatuses` does (pkg/kubelet/kubelet_pods.go:2600-2605).
pub fn map_container_status(
    cri: &v1::ContainerStatus,
    allocated: Option<&Container>,
    old: Option<&ContainerStatus>,
) -> ContainerStatus {
    let running = cri.state == v1::ContainerState::ContainerRunning as i32;
    let name = cri
        .metadata
        .as_ref()
        .map(|m| m.name.clone())
        .unwrap_or_default();
    let restart_count = cri.metadata.as_ref().map(|m| m.attempt).unwrap_or(0);

    ContainerStatus {
        name,
        ready: running,
        restart_count,
        state: Some(map_state(cri)),
        last_state: None,
        image: cri.image.as_ref().map(|i| i.image.clone()),
        image_id: empty_to_none(&cri.image_ref),
        container_id: empty_to_none(&cri.id),
        started: Some(running),
        // `status.AllocatedResources = allocatedContainer.Resources.Requests`
        // (kubelet_pods.go:2604).
        allocated_resources: allocated
            .and_then(|c| c.resources.as_ref())
            .and_then(|r| r.requests.clone()),
        allocated_resources_status: None,
        resources: allocated.and_then(|c| container_status_resources(c, cri, old)),
        user: None,
        volume_mounts: None,
        stop_signal: None,
    }
}
