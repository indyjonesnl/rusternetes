//! Map CRI container status onto rusternetes [`ContainerStatus`].
//!
//! Pure translation in the other direction from [`super::translate`]: the
//! runtime reports a `runtime.v1::ContainerStatus`, and the kubelet's pod-status
//! machinery wants a rusternetes `ContainerStatus`. Readiness/started here are
//! derived purely from the runtime state; the kubelet overlays probe results on
//! top.

use rusternetes_common::resources::pod::{ContainerState, ContainerStatus};
use rusternetes_cri::v1;

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
pub fn map_container_status(cri: &v1::ContainerStatus) -> ContainerStatus {
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
        allocated_resources: None,
        allocated_resources_status: None,
        resources: None,
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
        let s = map_container_status(&cri_status(v1::ContainerState::ContainerRunning));
        assert_eq!(s.name, "app");
        assert!(s.ready);
        assert_eq!(s.restart_count, 2);
        assert_eq!(s.container_id.as_deref(), Some("ctr-abc"));
        assert!(matches!(s.state, Some(ContainerState::Running { .. })));
    }

    #[test]
    fn created_maps_to_waiting_creating() {
        let s = map_container_status(&cri_status(v1::ContainerState::ContainerCreated));
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
        let s = map_container_status(&cri);
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
}
