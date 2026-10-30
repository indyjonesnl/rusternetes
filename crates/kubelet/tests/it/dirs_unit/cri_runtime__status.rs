//! Unit tests moved from `src/cri_runtime/status.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::quantity::{Format, Quantity};
    pub use rusternetes_common::resources::pod::{
        Container, ContainerState, ContainerStateRunning, ContainerStateTerminated,
        ContainerStateWaiting, ContainerStatus,
    };
    pub use rusternetes_common::types::ResourceRequirements;
    pub use rusternetes_cri::v1;
    pub use rusternetes_kubelet::cri_runtime::translate::{
        MILLI_CPU_TO_CPU, MIN_MILLI_CPU_LIMIT, MIN_SHARES, SHARES_PER_CPU,
    };
    pub use std::collections::HashMap;
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubelet::cri_runtime::status::*;

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
        assert!(matches!(
            s.state,
            Some(ContainerState {
                running: Some(ContainerStateRunning { .. }),
                ..
            })
        ));
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
            Some(ContainerState {
                waiting: Some(ContainerStateWaiting { reason, .. }),
                ..
            }) => {
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
            Some(ContainerState {
                terminated:
                    Some(ContainerStateTerminated {
                        exit_code,
                        finished_at,
                        ..
                    }),
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
