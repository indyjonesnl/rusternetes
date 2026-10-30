//! Unit tests moved from `src/runtime_state.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use chrono::{DateTime, Utc};
    pub use rusternetes_common::resources::NodeCondition;
    pub use rusternetes_cri::v1;
    pub use std::collections::HashMap;
    pub use std::sync::Mutex;
    pub use std::time::{Duration, Instant};
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubelet::runtime_state::*;

    fn status(rt: Option<bool>, net: Option<bool>) -> v1::StatusResponse {
        let mut conditions = Vec::new();
        for (ty, v) in [(RUNTIME_READY, rt), (NETWORK_READY, net)] {
            if let Some(status) = v {
                conditions.push(v1::RuntimeCondition {
                    r#type: ty.to_string(),
                    status,
                    reason: if status {
                        String::new()
                    } else {
                        "NetworkPluginNotReady".into()
                    },
                    message: if status {
                        String::new()
                    } else {
                        "no cni config".into()
                    },
                });
            }
        }
        v1::StatusResponse {
            status: Some(v1::RuntimeStatus { conditions }),
            ..Default::default()
        }
    }

    #[test]
    fn fresh_state_has_unknown_runtime_and_network() {
        let s = RuntimeState::new(MAX_WAIT_FOR_CONTAINER_RUNTIME);
        assert_eq!(
            s.runtime_errors().as_deref(),
            Some("container runtime status check may not have completed yet")
        );
        assert_eq!(s.network_errors().as_deref(), Some("network state unknown"));
    }

    #[test]
    fn ready_runtime_and_network_clear_errors() {
        let s = RuntimeState::new(MAX_WAIT_FOR_CONTAINER_RUNTIME);
        s.update_from_status(Ok(status(Some(true), Some(true))));
        assert_eq!(s.runtime_errors(), None);
        assert_eq!(s.network_errors(), None);
    }

    #[test]
    fn runtime_not_ready_reports_condition_and_does_not_sync() {
        let s = RuntimeState::new(MAX_WAIT_FOR_CONTAINER_RUNTIME);
        s.update_from_status(Ok(status(Some(false), Some(true))));
        assert_eq!(
            s.runtime_errors().as_deref(),
            Some("[container runtime status check may not have completed yet, container runtime not ready: RuntimeReady=false reason:NetworkPluginNotReady message:no cni config]")
        );
    }

    #[test]
    fn missing_runtime_ready_condition_is_an_error() {
        let s = RuntimeState::new(MAX_WAIT_FOR_CONTAINER_RUNTIME);
        s.update_from_status(Ok(status(None, Some(true))));
        assert!(s
            .runtime_errors()
            .unwrap()
            .contains("container runtime not ready: <nil>"));
    }

    #[test]
    fn network_not_ready_reports_condition() {
        let s = RuntimeState::new(MAX_WAIT_FOR_CONTAINER_RUNTIME);
        s.update_from_status(Ok(status(Some(true), Some(false))));
        assert_eq!(s.runtime_errors(), None);
        assert_eq!(
            s.network_errors().as_deref(),
            Some("container runtime network not ready: NetworkReady=false reason:NetworkPluginNotReady message:no cni config")
        );
    }

    /// `kubeletVolumeHost.SetKubeletError` -> `setStorageState`
    /// (volume_host.go:122): a set error is reported, `nil` clears it.
    #[test]
    fn storage_state_sets_and_clears() {
        let s = RuntimeState::new(MAX_WAIT_FOR_CONTAINER_RUNTIME);
        assert_eq!(s.storage_errors(), None);
        s.set_storage_state(Some("CSINode is not yet initialized".into()));
        assert_eq!(
            s.storage_errors().as_deref(),
            Some("CSINode is not yet initialized")
        );
        s.set_storage_state(None);
        assert_eq!(s.storage_errors(), None);
    }

    #[test]
    fn stale_sync_means_runtime_is_down() {
        let s = RuntimeState::new(Duration::ZERO);
        s.update_from_status(Ok(status(Some(true), Some(true))));
        assert_eq!(
            s.runtime_errors().as_deref(),
            Some("container runtime is down")
        );
    }

    #[test]
    fn status_rpc_error_leaves_state_untouched() {
        let s = RuntimeState::new(MAX_WAIT_FOR_CONTAINER_RUNTIME);
        s.update_from_status(Ok(status(Some(true), Some(true))));
        s.update_from_status(Err("boom".into()));
        assert_eq!(s.runtime_errors(), None);
    }

    fn cap(keys: &[&str]) -> HashMap<String, String> {
        keys.iter()
            .map(|k| (k.to_string(), "1".to_string()))
            .collect()
    }
    fn full() -> HashMap<String, String> {
        cap(&["cpu", "memory", "pods", "ephemeral-storage"])
    }
    fn cond(ready: bool, msg: &str, transition: DateTime<Utc>, hb: DateTime<Utc>) -> NodeCondition {
        NodeCondition {
            condition_type: "Ready".into(),
            status: if ready { "True" } else { "False" }.into(),
            last_heartbeat_time: Some(hb),
            last_transition_time: Some(transition),
            reason: Some(
                if ready {
                    "KubeletReady"
                } else {
                    "KubeletNotReady"
                }
                .into(),
            ),
            message: Some(msg.into()),
        }
    }

    type ReadyCase = (
        &'static str,
        Option<HashMap<String, String>>,
        Vec<NodeCondition>,
        Vec<String>,
        bool,
        NodeCondition,
        Option<&'static str>,
    );

    /// `TestReadyCondition` (nodestatus/setters_test.go:1188), the cases that
    /// do not need a container manager or shutdown manager.
    #[test]
    fn ready_condition_cases() {
        let now = Utc::now();
        let before = now - chrono::Duration::seconds(1);
        let ok = "kubelet is posting ready status";
        let cases: Vec<ReadyCase> = vec![
            (
                "new, ready",
                Some(full()),
                vec![],
                vec![],
                true,
                cond(true, ok, now, now),
                None,
            ),
            (
                "new, not ready: storage errors",
                Some(full()),
                vec![],
                vec!["some storage error".into()],
                true,
                cond(false, "some storage error", now, now),
                None,
            ),
            (
                "new, not ready: runtime and network errors",
                Some(full()),
                vec![],
                vec!["runtime".into(), "network".into()],
                true,
                cond(false, "[runtime, network]", now, now),
                None,
            ),
            (
                "new, not ready: missing capacities",
                None,
                vec![],
                vec![],
                true,
                cond(
                    false,
                    "missing node capacity for resources: cpu, memory, pods, ephemeral-storage",
                    now,
                    now,
                ),
                None,
            ),
            (
                "new, not ready: errors and missing capacities aggregate",
                Some(cap(&["cpu", "memory", "pods"])),
                vec![],
                vec!["runtime".into()],
                true,
                cond(
                    false,
                    "[runtime, missing node capacity for resources: ephemeral-storage]",
                    now,
                    now,
                ),
                None,
            ),
            (
                "new, ready: localStorageCapacityIsolation is not supported",
                Some(cap(&["cpu", "memory", "pods"])),
                vec![],
                vec![],
                false,
                cond(true, ok, now, now),
                None,
            ),
            (
                "transition to ready",
                Some(full()),
                vec![cond(false, "", before, before)],
                vec![],
                true,
                cond(true, ok, now, now),
                Some("NodeReady"),
            ),
            (
                "transition to not ready",
                Some(full()),
                vec![cond(true, "", before, before)],
                vec!["foo".into()],
                true,
                cond(false, "foo", now, now),
                Some("NodeNotReady"),
            ),
            (
                "ready, no transition",
                Some(full()),
                vec![cond(true, "", before, before)],
                vec![],
                true,
                cond(true, ok, before, now),
                None,
            ),
            (
                "not ready, no transition",
                Some(full()),
                vec![cond(false, "", before, before)],
                vec!["foo".into()],
                true,
                cond(false, "foo", before, now),
                None,
            ),
        ];
        for (desc, capacity, mut conds, errs, isolation, want, want_event) in cases {
            let upd = set_ready_condition(&mut conds, capacity.as_ref(), errs, now, isolation);
            assert_eq!(
                serde_json::to_value(&conds).unwrap(),
                serde_json::to_value(vec![want]).unwrap(),
                "{desc}"
            );
            assert_eq!(upd.event, want_event, "{desc}");
        }
    }

    #[test]
    fn ready_condition_flips_and_preserves_transition_time() {
        let t0 = Utc::now() - chrono::Duration::seconds(100);
        let t1 = Utc::now();
        let c = full();
        let mut conds = Vec::new();
        assert!(set_ready_condition(&mut conds, Some(&c), vec![], t0, true).changed);
        assert_eq!(conds[0].status, "True");
        // Same status: transition time preserved, heartbeat advances.
        assert!(!set_ready_condition(&mut conds, Some(&c), vec![], t1, true).changed);
        assert_eq!(conds[0].last_transition_time, Some(t0));
        assert_eq!(conds[0].last_heartbeat_time, Some(t1));
        // Failure flips to False with the aggregate message.
        assert!(
            set_ready_condition(&mut conds, Some(&c), vec!["a".into(), "b".into()], t1, true)
                .changed
        );
        assert_eq!(conds[0].status, "False");
        assert_eq!(conds[0].reason.as_deref(), Some("KubeletNotReady"));
        assert_eq!(conds[0].message.as_deref(), Some("[a, b]"));
        assert_eq!(conds[0].last_transition_time, Some(t1));
    }

    /// `recordNodeStatusEvent` (kubelet_node_status.go:632): a Normal
    /// `NodeNotReady` event on the Node with "Node %s status is now: %s".
    #[tokio::test]
    async fn node_status_event_is_recorded_on_the_node() {
        use rusternetes_storage::Storage;
        let storage = std::sync::Arc::new(rusternetes_storage::StorageBackend::new_memory());
        let recorder = rusternetes_storage::EventRecorder::new(std::sync::Arc::clone(&storage));
        record_node_status_event(&recorder, "node-1", EVENT_NODE_NOT_READY).await;
        let evs: Vec<rusternetes_common::resources::Event> =
            storage.list("/registry/events/").await.unwrap();
        assert_eq!(evs.len(), 1);
        assert_eq!(evs[0].reason, "NodeNotReady");
        assert_eq!(evs[0].message, "Node node-1 status is now: NodeNotReady");
        assert_eq!(evs[0].involved_object.kind.as_deref(), Some("Node"));
        assert_eq!(evs[0].involved_object.name.as_deref(), Some("node-1"));
        assert_eq!(evs[0].source.component, "kubelet");
        assert_eq!(evs[0].source.host.as_deref(), Some("node-1"));
    }
}
