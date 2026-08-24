//! Map CRI container status onto rusternetes [`ContainerStatus`].
//!
//! Pure translation in the other direction from [`super::translate`]: the
//! runtime reports a `runtime.v1::ContainerStatus`, and the kubelet's pod-status
//! machinery wants a rusternetes `ContainerStatus`. Readiness/started here are
//! derived purely from the runtime state; the kubelet overlays probe results on
//! top.

use rusternetes_common::quantity::{Format, Quantity};
use rusternetes_common::resources::pod::{Container, ContainerState, ContainerStatus};
use rusternetes_common::types::ResourceRequirements;
use rusternetes_cri::v1;
use std::collections::HashMap;

/// Kernel CFS constants, ported verbatim from upstream
/// `pkg/kubelet/cm/helpers_linux.go:43-59`.
mod cfs {
    pub const MIN_SHARES: i64 = 2;
    pub const SHARES_PER_CPU: i64 = 1024;
    pub const MILLI_CPU_TO_CPU: i64 = 1000;
    /// `MinQuotaPeriod * MilliCPUToCPU / QuotaPeriod`.
    pub const MIN_MILLI_CPU_LIMIT: i64 = 10;
}

/// Convert `cpu.shares` to milli-CPU. Port of upstream `sharesToMilliCPU`
/// (pkg/kubelet/cm/helpers_linux.go:383-389).
fn shares_to_milli_cpu(shares: i64) -> i64 {
    if shares < cfs::MIN_SHARES {
        return 0;
    }
    // ceil(shares * 1000 / 1024), integer-only to match Go's math.Ceil on the
    // float division without importing floating point. Both operands are
    // positive here, so the plain round-up form is exact.
    let numerator = shares * cfs::MILLI_CPU_TO_CPU;
    (numerator + cfs::SHARES_PER_CPU - 1) / cfs::SHARES_PER_CPU
}

/// Convert `cpu.cfs_quota_us`/`cpu.cfs_period_us` to milli-CPU. Port of
/// upstream `quotaToMilliCPU` (pkg/kubelet/cm/helpers_linux.go:394-399).
fn quota_to_milli_cpu(quota: i64, period: i64) -> i64 {
    if quota == -1 || period == 0 {
        return 0;
    }
    (quota * cfs::MILLI_CPU_TO_CPU) / period
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
            matches!(o.state, Some(ContainerState::Running { .. }))
                && o.container_id.as_deref() == Some(cri.id.as_str())
        })
        .and_then(|o| o.resources.as_ref());

    let mut resources = allocated_resources.clone();

    if let Some(limits) = resources.limits.as_mut() {
        match cpu_limit {
            // "If both the allocated & actual resources are at or below the
            // minimum effective limit, preserve the allocated value in the API
            // to avoid confusion and simplify comparisons." (:2369-2371)
            Some(milli)
                if milli > cfs::MIN_MILLI_CPU_LIMIT
                    || limits
                        .get("cpu")
                        .and_then(|c| milli_cpu_of(c))
                        .is_some_and(|a| a > cfs::MIN_MILLI_CPU_LIMIT) =>
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
                if milli > cfs::MIN_SHARES
                    || requests
                        .get("cpu")
                        .and_then(|c| milli_cpu_of(c))
                        .is_some_and(|a| a > cfs::MIN_SHARES) =>
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
fn nanos_to_rfc3339(nanos: i64) -> Option<String> {
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
        v1::ContainerState::ContainerCreated => ContainerState::Waiting {
            reason: Some("ContainerCreating".to_string()),
            message: empty_to_none(&cri.message),
        },
        v1::ContainerState::ContainerRunning => ContainerState::Running {
            started_at: nanos_to_rfc3339(cri.started_at),
        },
        v1::ContainerState::ContainerExited => ContainerState::Terminated {
            exit_code: cri.exit_code,
            signal: None,
            reason: empty_to_none(&cri.reason),
            message: empty_to_none(&cri.message),
            started_at: nanos_to_rfc3339(cri.started_at),
            finished_at: nanos_to_rfc3339(cri.finished_at),
            container_id: empty_to_none(&cri.id),
        },
        v1::ContainerState::ContainerUnknown => ContainerState::Waiting {
            reason: Some("Unknown".to_string()),
            message: empty_to_none(&cri.message),
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

#[cfg(test)]
mod tests {
    use super::*;

    fn cri_status(state: v1::ContainerState) -> v1::ContainerStatus {
        v1::ContainerStatus {
            id: "ctr-abc".to_string(),
            metadata: Some(v1::ContainerMetadata {
                name: "app".to_string(),
                attempt: 2,
            }),
            state: state as i32,
            exit_code: 0,
            ..Default::default()
        }
    }

    /// A spec container requesting 100m/100Mi with 200m/200Mi limits.
    fn allocated_container() -> Container {
        Container {
            name: "app".to_string(),
            resources: Some(ResourceRequirements {
                requests: Some(HashMap::from([
                    ("cpu".to_string(), "100m".to_string()),
                    ("memory".to_string(), "100Mi".to_string()),
                ])),
                limits: Some(HashMap::from([
                    ("cpu".to_string(), "200m".to_string()),
                    ("memory".to_string(), "200Mi".to_string()),
                ])),
                claims: None,
            }),
            ..Default::default()
        }
    }

    fn get(map: &Option<HashMap<String, String>>, key: &str) -> Option<String> {
        map.as_ref().and_then(|m| m.get(key)).cloned()
    }

    /// Regression for the six `[sig-node] Pod InPlace Resize Container
    /// [Conformance]` specs, which all `[PANICKED]` with a nil-pointer
    /// dereference at `creating and verifying pod` — before any resize.
    ///
    /// The e2e framework dereferences the pointer unconditionally:
    ///
    /// ```text
    /// // test/e2e/common/node/framework/podresize/resize.go:230
    /// if err := framework.Gomega().Expect(*gotCtrStatus.Resources)...
    /// ```
    ///
    /// so a missing `status.containerStatuses[].resources` is not a mismatch,
    /// it is a panic. Upstream always populates it for a container that has an
    /// allocated spec — `convertToAPIContainerStatuses`
    /// (pkg/kubelet/kubelet_pods.go:2600-2605).
    #[test]
    fn running_status_reports_allocated_and_actual_resources() {
        let allocated = allocated_container();
        let mut cri = cri_status(v1::ContainerState::ContainerRunning);
        cri.resources = Some(v1::ContainerResources {
            linux: Some(v1::LinuxContainerResources {
                // quotaToMilliCPU(20000, 100000) == 200m
                cpu_period: 100_000,
                cpu_quota: 20_000,
                // sharesToMilliCPU(102) == ceil(102 * 1000 / 1024) == 100m
                cpu_shares: 102,
                memory_limit_in_bytes: 209_715_200,
                ..Default::default()
            }),
            ..Default::default()
        });

        let s = map_container_status(&cri, Some(&allocated), None);

        let res = s
            .resources
            .expect("a running container MUST report status.resources");
        assert_eq!(get(&res.requests, "cpu").as_deref(), Some("100m"));
        assert_eq!(get(&res.limits, "cpu").as_deref(), Some("200m"));
        assert_eq!(get(&res.limits, "memory").as_deref(), Some("200Mi"));

        // AllocatedResources mirrors the allocated *requests* verbatim.
        assert_eq!(
            get(&s.allocated_resources, "cpu").as_deref(),
            Some("100m"),
            "status.allocatedResources must equal the allocated requests"
        );
        assert_eq!(
            get(&s.allocated_resources, "memory").as_deref(),
            Some("100Mi")
        );
    }

    /// "If the container isn't running, just use the allocated resources."
    /// — pkg/kubelet/kubelet_pods.go:2357-2359.
    #[test]
    fn non_running_status_reports_allocated_resources_verbatim() {
        let allocated = allocated_container();
        let cri = cri_status(v1::ContainerState::ContainerExited);

        let s = map_container_status(&cri, Some(&allocated), None);

        let res = s
            .resources
            .expect("an exited container MUST still report status.resources");
        assert_eq!(get(&res.requests, "cpu").as_deref(), Some("100m"));
        assert_eq!(get(&res.limits, "memory").as_deref(), Some("200Mi"));
    }

    /// The runtime may report no `resources` at all (a CRI implementation is
    /// not required to fill it in). The allocated values must still surface —
    /// nil is what panics the framework.
    #[test]
    fn running_status_without_cri_resources_falls_back_to_allocated() {
        let allocated = allocated_container();
        let cri = cri_status(v1::ContainerState::ContainerRunning);

        let s = map_container_status(&cri, Some(&allocated), None);

        let res = s
            .resources
            .expect("missing CRI resources must not yield a nil status.resources");
        assert_eq!(get(&res.requests, "cpu").as_deref(), Some("100m"));
        assert_eq!(get(&res.limits, "cpu").as_deref(), Some("200m"));
    }

    /// A container with no `resources` in its spec has nothing to report:
    /// upstream leaves `status.Resources` nil, and the resize framework only
    /// dereferences it for pods it created with resources set.
    #[test]
    fn container_without_spec_resources_reports_none() {
        let allocated = Container {
            name: "app".to_string(),
            ..Default::default()
        };
        let cri = cri_status(v1::ContainerState::ContainerRunning);

        let s = map_container_status(&cri, Some(&allocated), None);

        assert!(s.resources.is_none());
        assert!(s.allocated_resources.is_none());
    }

    #[test]
    fn termination_message_from_file_on_success() {
        // #442: pod succeeds (exit 0), policy FallbackToLogsOnError, file has
        // content -> message MUST be the file content (file wins over logs).
        let msg = resolve_termination_message(
            Some("DONE".to_string()),
            "FallbackToLogsOnError",
            0,
            Some("Completed"),
            || panic!("logs must not be read when the file is readable"),
        );
        assert_eq!(msg.as_deref(), Some("DONE"));
    }

    #[test]
    fn termination_message_file_wins_for_file_policy() {
        let msg = resolve_termination_message(
            Some("from-file".to_string()),
            "File",
            1,
            Some("Error"),
            || Some("from-logs".to_string()),
        );
        assert_eq!(msg.as_deref(), Some("from-file"));
    }

    #[test]
    fn termination_message_empty_file_fallback_to_logs_on_error() {
        // Regression for [sig-node] ... FallbackToLogsOnError conformance test.
        // Upstream `getTerminationMessage` (pkg/kubelet/kuberuntime/
        // kuberuntime_container.go:583):
        //   return string(data), (fallbackToLogs && len(data) == 0)
        // When the termination-log file is empty (the container only wrote to
        // stdout, not the log file) AND the policy is FallbackToLogsOnError AND
        // the exit was non-zero, `checkLogs=true` → the log tail is used as the
        // termination message.
        let msg = resolve_termination_message(
            Some(String::new()),
            "FallbackToLogsOnError",
            1,
            Some("Error"),
            || Some("DONE".to_string()),
        );
        assert_eq!(
            msg.as_deref(),
            Some("DONE"),
            "empty termination-log + FallbackToLogsOnError + exit!=0 must use log tail"
        );
    }

    #[test]
    fn termination_message_empty_file_file_policy_yields_none() {
        // Default "File" policy: empty file yields no message (no log fallback).
        let msg =
            resolve_termination_message(Some(String::new()), "File", 1, Some("Error"), || {
                panic!("File policy must not read logs")
            });
        assert_eq!(msg, None);
    }

    #[test]
    fn termination_message_empty_file_clean_exit_yields_none() {
        // FallbackToLogsOnError but exit 0 and non-OOMKilled: no log fallback
        // even for an empty file.
        let msg = resolve_termination_message(
            Some(String::new()),
            "FallbackToLogsOnError",
            0,
            Some("Completed"),
            || panic!("no fallback on clean exit"),
        );
        assert_eq!(msg, None);
    }

    #[test]
    fn termination_message_fallback_to_logs_on_error() {
        // File unreadable + FallbackToLogsOnError + non-zero exit -> log tail.
        let msg =
            resolve_termination_message(None, "FallbackToLogsOnError", 1, Some("Error"), || {
                Some("boom from logs".to_string())
            });
        assert_eq!(msg.as_deref(), Some("boom from logs"));
    }

    #[test]
    fn termination_message_no_fallback_on_clean_exit() {
        // FallbackToLogsOnError but exit 0 and no file -> no log fallback.
        let msg = resolve_termination_message(
            None,
            "FallbackToLogsOnError",
            0,
            Some("Completed"),
            || Some("logs".to_string()),
        );
        assert_eq!(msg, None);
    }

    #[test]
    fn termination_message_fallback_on_oomkilled() {
        // OOMKilled counts as an error case for the log fallback even at exit 0.
        let msg = resolve_termination_message(
            None,
            "FallbackToLogsOnError",
            0,
            Some("OOMKilled"),
            || Some("oom logs".to_string()),
        );
        assert_eq!(msg.as_deref(), Some("oom logs"));
    }

    #[test]
    fn termination_message_file_policy_never_reads_logs() {
        // Default "File" policy: an unreadable file yields no message, never logs.
        let msg = resolve_termination_message(None, "File", 1, Some("Error"), || {
            panic!("File policy must not read logs")
        });
        assert_eq!(msg, None);
    }

    #[test]
    fn termination_message_truncates_to_tail() {
        let big = "x".repeat(MAX_TERMINATION_MESSAGE_LENGTH + 100) + "TAIL";
        let msg = resolve_termination_message(Some(big), "File", 0, None, || None).unwrap();
        assert_eq!(msg.len(), MAX_TERMINATION_MESSAGE_LENGTH);
        assert!(msg.ends_with("TAIL"), "must keep the trailing bytes");
    }

    #[test]
    fn running_maps_to_ready_running() {
        let s = map_container_status(
            &cri_status(v1::ContainerState::ContainerRunning),
            None,
            None,
        );
        assert_eq!(s.name, "app");
        assert!(s.ready);
        assert_eq!(s.restart_count, 2);
        assert_eq!(s.container_id.as_deref(), Some("ctr-abc"));
        assert!(matches!(s.state, Some(ContainerState::Running { .. })));
    }

    #[test]
    fn created_maps_to_waiting_creating() {
        let s = map_container_status(
            &cri_status(v1::ContainerState::ContainerCreated),
            None,
            None,
        );
        assert!(!s.ready);
        match s.state {
            Some(ContainerState::Waiting { reason, .. }) => {
                assert_eq!(reason.as_deref(), Some("ContainerCreating"));
            }
            other => panic!("expected Waiting, got {other:?}"),
        }
    }

    #[test]
    fn exited_maps_to_terminated_with_exit_code() {
        let mut cri = cri_status(v1::ContainerState::ContainerExited);
        cri.exit_code = 137;
        cri.finished_at = 1_700_000_000_000_000_000;
        let s = map_container_status(&cri, None, None);
        assert!(!s.ready);
        match s.state {
            Some(ContainerState::Terminated {
                exit_code,
                finished_at,
                ..
            }) => {
                assert_eq!(exit_code, 137);
                assert!(finished_at.is_some(), "finished_at should be set");
            }
            other => panic!("expected Terminated, got {other:?}"),
        }
    }

    #[test]
    fn zero_timestamp_is_none() {
        assert_eq!(nanos_to_rfc3339(0), None);
        assert!(nanos_to_rfc3339(1_700_000_000_000_000_000).is_some());
    }

    /// Regression for "[sig-node] Probing container should have monotonically
    /// increasing restart count".
    ///
    /// `map_container_status` derives `restart_count` from `metadata.attempt`
    /// which is the value the kubelet stamps into the container at creation time
    /// (matching upstream `startContainer` which passes `restartCount` into
    /// `generateContainerConfig` → CRI `ContainerMetadata.attempt`):
    /// ```go
    /// // pkg/kubelet/kuberuntime/kuberuntime_container.go:371
    /// Attempt: restartCountUint32,
    /// ```
    /// When the kubelet correctly stamps `attempt = prev_max + 1`, the reported
    /// restartCount is monotonic. This test verifies that `map_container_status`
    /// faithfully reads the `attempt` field and never resets it.
    #[test]
    fn restart_count_is_monotonic_from_cri_attempt() {
        // First run: attempt=0 → restartCount 0.
        let mut cri = cri_status(v1::ContainerState::ContainerRunning);
        cri.metadata = Some(v1::ContainerMetadata {
            name: "app".to_string(),
            attempt: 0,
        });
        let s0 = map_container_status(&cri, None, None);
        assert_eq!(s0.restart_count, 0, "first run: restartCount must be 0");

        // After one restart: attempt=1 → restartCount 1 (never regresses to 0).
        cri.metadata = Some(v1::ContainerMetadata {
            name: "app".to_string(),
            attempt: 1,
        });
        let s1 = map_container_status(&cri, None, None);
        assert_eq!(
            s1.restart_count, 1,
            "after first restart: restartCount must be 1"
        );
        assert!(
            s1.restart_count >= s0.restart_count,
            "restartCount must be monotonically non-decreasing: {} >= {}",
            s1.restart_count,
            s0.restart_count
        );

        // After second restart: attempt=2 → restartCount 2.
        cri.metadata = Some(v1::ContainerMetadata {
            name: "app".to_string(),
            attempt: 2,
        });
        let s2 = map_container_status(&cri, None, None);
        assert_eq!(
            s2.restart_count, 2,
            "after second restart: restartCount must be 2"
        );
        assert!(
            s2.restart_count >= s1.restart_count,
            "restartCount must be monotonically non-decreasing: {} >= {}",
            s2.restart_count,
            s1.restart_count
        );
    }
}
