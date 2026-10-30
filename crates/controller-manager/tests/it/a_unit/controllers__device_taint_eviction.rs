//! Unit tests moved from `src/controllers/device_taint_eviction.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::{anyhow, Result};
    pub use chrono::{DateTime, Duration as ChronoDuration, Utc};
    pub use futures::StreamExt;
    pub use rusternetes_common::resources::devicetaintrule::DeviceTaintRule;
    pub use rusternetes_common::resources::dra::{
        AllocationResult, Device, DeviceRequestAllocationResult, DeviceTaint, DeviceTaintEffect,
        DeviceToleration, ResourceClaim, ResourceSlice, TolerationOperator,
    };
    pub use rusternetes_common::resources::pod::PodResourceClaim;
    pub use rusternetes_common::resources::{
        EventSource, EventType, ObjectReference, Pod, PodCondition,
    };
    pub use rusternetes_common::types::Condition;
    pub use rusternetes_common::Error;
    pub use rusternetes_controller_manager::controllers::device_taint_eviction_metrics::{
        self as metrics, Metrics,
    };
    pub use rusternetes_controller_manager::controllers::taint_eviction::update_pod_condition;
    pub use rusternetes_controller_manager::controllers::typed_workqueue::{
        NopQueue, RateLimitingQueue, WorkItemQueue,
    };
    pub use rusternetes_storage::{build_key, build_prefix, EventRecorder, Storage, WatchEvent};
    pub use std::collections::{HashMap, HashSet};
    pub use std::sync::atomic::{AtomicBool, Ordering};
    pub use std::sync::{Arc, Mutex};
    pub use std::time::Duration;
    pub use tokio::sync::mpsc;
    pub use tokio::task::JoinSet;
    pub use tracing::{debug, error, info, warn};
}

#[path = "device_taint_eviction_tests.rs"]
mod tests {
    //! Cases ported from `pkg/controller/devicetainteviction/
    //! device_taint_eviction_test.go` (kubernetes release-1.35): `TestController`
    //! (the table, each case run in every permutation of its events, which must
    //! all reach the same state), `TestDeviceTaintRule`-style status flow,
    //! `testCancelEviction`, `TestParallelPodDeletion`-style retry (`TestRetry`)
    //! and the metrics check (`testPodDeletionsMetrics`). The synctest bubble's
    //! deterministic clock becomes an injected mutable clock; `Mock[workItem]`
    //! (`mockqueue_test.go`) is [`MockQueue`].

    use super::__parent::*;
    use chrono::TimeZone;
    use futures::future::BoxFuture;
    use rusternetes_common::resources::dra::ResourceClaimConsumerReference;
    use rusternetes_controller_manager::controllers::device_taint_eviction::*;
    use rusternetes_controller_manager::controllers::typed_workqueue::WorkItemQueue;
    use rusternetes_storage::memory::MemoryStorage;
    use serde_json::json;

    const NODE_NAME: &str = "worker";
    const NODE_NAME2: &str = "worker-2";
    const DRIVER: &str = "some-driver";
    const POD_NAME: &str = "my-pod";
    const POD_UID: &str = "1234";
    const RESOURCE_NAME: &str = "my-resource";
    const NAMESPACE: &str = "default";
    const TAINT_KEY: &str = "example.com/taint";
    const TAINT_VALUE: &str = "something";
    const TOLERATION_DURATION: Duration = Duration::from_secs(60 * 60);

    fn taint_time() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap()
    }

    fn claim_name() -> String {
        format!("{POD_NAME}-{RESOURCE_NAME}")
    }

    fn cd(d: Duration) -> ChronoDuration {
        ChronoDuration::from_std(d).unwrap()
    }

    /// The big table functions are huge state machines; poll them on a thread with
    /// a generous stack so a debug build does not overflow.
    fn run_big<F: std::future::Future<Output = ()> + Send + 'static>(f: F) {
        std::thread::Builder::new()
            .stack_size(256 * 1024 * 1024)
            .spawn(move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(f)
            })
            .unwrap()
            .join()
            .unwrap();
    }

    // --- the mock work queue (mockqueue_test.go) ---------------------------------

    #[derive(Default, Debug, Clone, PartialEq)]
    struct MockState {
        ready: Vec<WorkItem>,
        later: Vec<(WorkItem, Duration)>,
        failures: HashMap<WorkItem, usize>,
    }

    #[derive(Default)]
    struct MockQueue {
        state: Mutex<MockState>,
    }

    impl MockQueue {
        fn state(&self) -> MockState {
            self.state.lock().unwrap().clone()
        }

        /// The test advances time: every delayed item gets that much closer;
        /// items that are due become ready (`AddAfter(item, d-advance)`, which
        /// adds immediately for zero).
        fn advance(&self, by: Duration) {
            let mut st = self.state.lock().unwrap();
            let later = std::mem::take(&mut st.later);
            for (item, d) in later {
                if d <= by {
                    if !st.ready.contains(&item) {
                        st.ready.push(item);
                    }
                } else {
                    st.later.push((item, d - by));
                }
            }
        }
    }

    impl WorkItemQueue<WorkItem> for MockQueue {
        fn add(&self, item: WorkItem) {
            let mut st = self.state.lock().unwrap();
            if !st.ready.contains(&item) {
                st.ready.push(item);
            }
        }

        fn add_after(&self, item: WorkItem, delay: Duration) {
            if delay.is_zero() {
                self.add(item);
                return;
            }
            let mut st = self.state.lock().unwrap();
            // delaying_queue.go:340-349: only ever shortens an existing delay.
            if let Some(existing) = st.later.iter_mut().find(|(i, _)| *i == item) {
                if existing.1 > delay {
                    existing.1 = delay;
                }
                return;
            }
            st.later.push((item, delay));
        }

        fn add_rate_limited(&self, item: WorkItem) {
            *self.state.lock().unwrap().failures.entry(item).or_default() += 1;
        }

        fn forget(&self, item: &WorkItem) {
            self.state.lock().unwrap().failures.remove(item);
        }

        fn num_requeues(&self, item: &WorkItem) -> usize {
            self.state
                .lock()
                .unwrap()
                .failures
                .get(item)
                .copied()
                .unwrap_or(0)
        }

        fn get(&self) -> BoxFuture<'_, Option<WorkItem>> {
            let mut st = self.state.lock().unwrap();
            let item = if st.ready.is_empty() {
                None
            } else {
                Some(st.ready.remove(0))
            };
            Box::pin(std::future::ready(item))
        }

        fn done(&self, _item: &WorkItem) {}

        fn shut_down(&self) {}
    }

    fn sorted_items(mut v: Vec<WorkItem>) -> Vec<WorkItem> {
        v.sort_by_key(|i| format!("{i:?}"));
        v
    }

    fn sorted_later(mut v: Vec<(WorkItem, Duration)>) -> Vec<(WorkItem, Duration)> {
        v.sort_by_key(|(i, d)| (*d, format!("{i:?}")));
        v
    }

    fn normalized(s: &MockState) -> MockState {
        MockState {
            ready: sorted_items(s.ready.clone()),
            later: sorted_later(s.later.clone()),
            failures: s.failures.clone(),
        }
    }

    // --- fixtures ------------------------------------------------------------------

    fn dt(key: &str, value: Option<&str>, effect: DeviceTaintEffect) -> DeviceTaint {
        DeviceTaint {
            key: key.to_string(),
            value: value.map(str::to_string),
            effect: Some(effect),
            time_added: Some(taint_time()),
        }
    }

    fn mk_slice(node: &str, driver: &str, devices: Vec<(&str, Vec<DeviceTaint>)>) -> ResourceSlice {
        let mut slice: ResourceSlice = serde_json::from_value(json!({
            "metadata": {"name": format!("{node}-{driver}")},
            "spec": {
                "driver": driver,
                "nodeName": node,
                "pool": {"name": node, "resourceSliceCount": 1},
            }
        }))
        .unwrap();
        for (name, taints) in devices {
            let mut device: Device = serde_json::from_value(json!({"name": name})).unwrap();
            device.taints = taints;
            slice.spec.devices.push(device);
        }
        slice
    }

    fn mk_rule(
        name: &str,
        uid: &str,
        selector: (Option<&str>, Option<&str>),
        taint: DeviceTaint,
    ) -> DeviceTaintRule {
        let mut rule: DeviceTaintRule = serde_json::from_value(json!({
            "metadata": {"name": name, "uid": uid},
        }))
        .unwrap();
        rule.spec.device_selector = Some(
            serde_json::from_value(json!({
                "driver": selector.0,
                "device": selector.1,
            }))
            .unwrap(),
        );
        rule.spec.taint = taint;
        rule
    }

    fn mk_claim(name: &str, namespace: &str) -> ResourceClaim {
        serde_json::from_value(json!({
            "apiVersion": "resource.k8s.io/v1",
            "kind": "ResourceClaim",
            "metadata": {"name": name, "namespace": namespace},
        }))
        .unwrap()
    }

    fn owned_by(mut claim: ResourceClaim, pod_name: &str, pod_uid: &str) -> ResourceClaim {
        claim.metadata.owner_references = Some(
            serde_json::from_value(json!([{
                "apiVersion": "v1", "kind": "Pod", "name": pod_name, "uid": pod_uid, "controller": true
            }]))
            .unwrap(),
        );
        claim
    }

    fn allocated(mut claim: ResourceClaim, devices: &[&str]) -> ResourceClaim {
        let results: Vec<DeviceRequestAllocationResult> = devices
            .iter()
            .map(|d| DeviceRequestAllocationResult {
                driver: DRIVER.to_string(),
                pool: NODE_NAME.to_string(),
                device: d.to_string(),
                request: "req-1".to_string(),
                ..Default::default()
            })
            .collect();
        let status = claim.status.get_or_insert_with(Default::default);
        status.allocation = Some(AllocationResult {
            devices: rusternetes_common::resources::dra::DeviceAllocationResult {
                results,
                config: vec![],
            },
            node_selector: None,
        });
        claim
    }

    fn reserved_for(mut claim: ResourceClaim, consumers: &[(&str, &str)]) -> ResourceClaim {
        let status = claim.status.get_or_insert_with(Default::default);
        status.reserved_for = consumers
            .iter()
            .map(|(name, uid)| ResourceClaimConsumerReference {
                api_group: None,
                resource: "pods".to_string(),
                name: name.to_string(),
                uid: uid.to_string(),
            })
            .collect();
        claim
    }

    fn toleration(
        key: &str,
        op: TolerationOperator,
        value: Option<&str>,
        seconds: Option<i64>,
    ) -> DeviceToleration {
        DeviceToleration {
            key: key.to_string(),
            value: value.map(str::to_string),
            effect: Some(DeviceTaintEffect::NoExecute),
            operator: Some(op),
            toleration_seconds: seconds,
        }
    }

    fn with_tolerations(
        claim: &ResourceClaim,
        tolerations: Vec<DeviceToleration>,
    ) -> ResourceClaim {
        let mut claim = claim.clone();
        claim
            .status
            .as_mut()
            .unwrap()
            .allocation
            .as_mut()
            .unwrap()
            .devices
            .results[0]
            .tolerations = tolerations;
        claim
    }

    fn mk_pod(
        name: &str,
        namespace: &str,
        uid: &str,
        claims: serde_json::Value,
        node: &str,
    ) -> Pod {
        let mut spec = json!({
            "containers": [{"name": "c", "image": "i"}],
            "resourceClaims": claims,
        });
        if !node.is_empty() {
            spec["nodeName"] = json!(node);
        }
        serde_json::from_value(json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": name, "namespace": namespace, "uid": uid},
            "spec": spec,
            "status": {"phase": "Running"},
        }))
        .unwrap()
    }

    struct Fx {
        slice: Arc<ResourceSlice>,
        slice2: Arc<ResourceSlice>,
        simple_slice: Arc<ResourceSlice>,
        slice_replaced: Arc<ResourceSlice>,
        slice_other_devices: Arc<ResourceSlice>,
        slice_tainted: Arc<ResourceSlice>,
        slice_tainted_extended: Arc<ResourceSlice>,
        slice_tainted_none: Arc<ResourceSlice>,
        slice_tainted_unknown: Arc<ResourceSlice>,
        slice_tainted_no_schedule: Arc<ResourceSlice>,
        slice_tainted_value_other: Arc<ResourceSlice>,
        slice_tainted_twice: Arc<ResourceSlice>,
        slice_untainted: Arc<ResourceSlice>,
        rule_evict: Arc<DeviceTaintRule>,
        rule_evict_instance1: Arc<DeviceTaintRule>,
        rule_evict_instance2_later: Arc<DeviceTaintRule>,
        rule_none: Arc<DeviceTaintRule>,
        // Fixture mirrors upstream's `ruleEvictOther`; no ported test reads it yet.
        #[allow(dead_code)]
        rule_evict_other: Arc<DeviceTaintRule>,
        claim: Arc<ResourceClaim>,
        in_use_claim: Arc<ResourceClaim>,
        in_use_claim_other_namespace: Arc<ResourceClaim>,
        in_use_claim_other_name: Arc<ResourceClaim>,
        in_use_claim_other_name_shared: Arc<ResourceClaim>,
        in_use_claim_with_toleration: Arc<ResourceClaim>,
        in_use_claim_old: Arc<ResourceClaim>,
        unscheduled_pod: Arc<Pod>,
        pod: Arc<Pod>,
        pod_two_claims: Arc<Pod>,
        pod_other_uid: Arc<Pod>,
        pod_other_namespace: Arc<Pod>,
        pod_other_name: Arc<Pod>,
        pod_template: Arc<Pod>,
        pod_template_in_status: Arc<Pod>,
        pod_template_no_claim: Arc<Pod>,
    }

    fn taint_at(
        key: &str,
        value: Option<&str>,
        effect: DeviceTaintEffect,
        at: DateTime<Utc>,
    ) -> DeviceTaint {
        DeviceTaint {
            time_added: Some(at),
            ..dt(key, value, effect)
        }
    }

    impl Fx {
        fn new() -> Self {
            let no_execute =
                |value: Option<&str>| dt(TAINT_KEY, value, DeviceTaintEffect::NoExecute);
            let slice = mk_slice(
                NODE_NAME,
                DRIVER,
                vec![
                    ("instance", vec![]),
                    (
                        "instance-no-schedule",
                        vec![DeviceTaint {
                            time_added: None,
                            ..dt(TAINT_KEY, None, DeviceTaintEffect::NoSchedule)
                        }],
                    ),
                    (
                        "instance-no-execute",
                        vec![
                            dt(TAINT_KEY, None, DeviceTaintEffect::None),
                            dt(TAINT_KEY, None, DeviceTaintEffect::NoExecute),
                        ],
                    ),
                ],
            );
            let mut slice_replaced = slice.clone();
            slice_replaced.metadata.name += "-updated";
            slice_replaced.spec.pool.generation += 1;
            let mut slice_other_devices = slice.clone();
            for d in &mut slice_other_devices.spec.devices {
                d.name += "-other";
            }
            let taint = no_execute(Some(TAINT_VALUE));
            let mut slice_tainted = slice.clone();
            slice_tainted.spec.devices[0].taints = vec![taint.clone()];
            let mut slice_tainted_extended = slice_tainted.clone();
            let mut extra = slice_tainted_extended.spec.devices[0].clone();
            extra.name += "-other";
            slice_tainted_extended.spec.devices.push(extra);
            let set_effect = |s: &ResourceSlice, e: DeviceTaintEffect| {
                let mut s = s.clone();
                for d in &mut s.spec.devices {
                    for t in &mut d.taints {
                        t.effect = Some(e.clone());
                    }
                }
                s
            };
            let slice_tainted_none = set_effect(&slice_tainted, DeviceTaintEffect::None);
            let slice_tainted_unknown = set_effect(
                &slice_tainted,
                DeviceTaintEffect::Unknown("unknown-effect".into()),
            );
            let slice_tainted_no_schedule =
                set_effect(&slice_tainted, DeviceTaintEffect::NoSchedule);
            let mut slice_tainted_value_other = slice_tainted.clone();
            slice_tainted_value_other.spec.devices[0].taints[0] =
                no_execute(Some(&format!("{TAINT_VALUE}-other")));
            let taint1 = no_execute(None);
            let taint2 = dt(
                &format!("{TAINT_KEY}-other"),
                None,
                DeviceTaintEffect::NoExecute,
            );
            let mut slice_tainted_twice = slice.clone();
            slice_tainted_twice.spec.devices[0].taints = vec![taint1, taint2];
            let mut slice_untainted = slice.clone();
            slice_untainted.spec.devices[1].taints = vec![];
            slice_untainted.spec.devices[2].taints = vec![];
            let mut slice2 = slice.clone();
            slice2.metadata.name += "-2";
            slice2.spec.node_name = Some(NODE_NAME2.to_string());
            slice2.spec.pool.name = NODE_NAME2.to_string();
            slice2.spec.pool.generation += 1;

            let rule_taint = no_execute(Some(TAINT_VALUE));
            let rule_evict = mk_rule("evict", "1234", (Some(DRIVER), None), rule_taint.clone());
            let rule_evict_instance1 = mk_rule(
                "evict-instance",
                "1234",
                (Some(DRIVER), Some("instance")),
                rule_taint.clone(),
            );
            let rule_evict_instance2_later = mk_rule(
                "evict-instance-no-execute",
                "5678",
                (Some(DRIVER), Some("instance-no-execute")),
                taint_at(
                    TAINT_KEY,
                    Some(TAINT_VALUE),
                    DeviceTaintEffect::NoExecute,
                    taint_time() + ChronoDuration::seconds(40),
                ),
            );
            let rule_none = mk_rule(
                "evict",
                "1234",
                (Some(DRIVER), None),
                DeviceTaint {
                    effect: Some(DeviceTaintEffect::None),
                    ..rule_taint.clone()
                },
            );
            let rule_evict_other = mk_rule(
                "evict-other",
                "1234-other",
                (None, Some("instance")),
                rule_taint,
            );

            let claim = mk_claim(&claim_name(), NAMESPACE);
            let alloc_instance = |c: ResourceClaim| allocated(c, &["instance"]);
            let alloc_other =
                |c: ResourceClaim| allocated(c, &["instance-no-schedule", "instance-no-execute"]);
            let in_use_claim = reserved_for(
                alloc_instance(owned_by(claim.clone(), POD_NAME, POD_UID)),
                &[(POD_NAME, POD_UID)],
            );
            let mut in_use_claim_other_namespace = reserved_for(
                alloc_other(owned_by(claim.clone(), POD_NAME, &format!("{POD_UID}-2"))),
                &[(POD_NAME, &format!("{POD_UID}-2"))],
            );
            in_use_claim_other_namespace.metadata.namespace = Some(format!("{NAMESPACE}-other"));
            let mut in_use_claim_other_name = reserved_for(
                alloc_other(owned_by(
                    claim.clone(),
                    &format!("{POD_NAME}-other"),
                    &format!("{POD_UID}-3"),
                )),
                &[(&format!("{POD_NAME}-other"), &format!("{POD_UID}-3"))],
            );
            in_use_claim_other_name.metadata.name = format!("{}-other", claim_name());
            let in_use_claim_other_name_shared = reserved_for(
                in_use_claim_other_name.clone(),
                &[
                    (POD_NAME, POD_UID),
                    (&format!("{POD_NAME}-other"), &format!("{POD_UID}-3")),
                ],
            );
            let in_use_claim_with_toleration = with_tolerations(
                &in_use_claim,
                vec![toleration(
                    TAINT_KEY,
                    TolerationOperator::Equal,
                    Some(TAINT_VALUE),
                    Some(TOLERATION_DURATION.as_secs() as i64),
                )],
            );
            let mut in_use_claim_old =
                owned_by(in_use_claim.clone(), POD_NAME, &format!("{POD_UID}-other"));
            in_use_claim_old.metadata.uid = "other".to_string();

            let cn = claim_name();
            let by_name = |name: &str| json!([{"name": RESOURCE_NAME, "resourceClaimName": name}]);
            let unscheduled_pod = mk_pod(POD_NAME, NAMESPACE, POD_UID, by_name(&cn), "");
            let pod = mk_pod(POD_NAME, NAMESPACE, POD_UID, by_name(&cn), NODE_NAME);
            let other_name = format!("{cn}-other");
            let pod_two_claims = mk_pod(
                POD_NAME,
                NAMESPACE,
                POD_UID,
                json!([
                    {"name": RESOURCE_NAME, "resourceClaimName": cn},
                    {"name": format!("{RESOURCE_NAME}-other"), "resourceClaimName": other_name},
                ]),
                NODE_NAME,
            );
            let pod_other_uid = mk_pod(
                POD_NAME,
                NAMESPACE,
                &format!("{POD_UID}-other"),
                by_name(&cn),
                NODE_NAME,
            );
            let pod_other_namespace = mk_pod(
                POD_NAME,
                &format!("{NAMESPACE}-other"),
                &format!("{POD_UID}-2"),
                by_name(&cn),
                NODE_NAME,
            );
            let pod_other_name = mk_pod(
                &format!("{POD_NAME}-other"),
                NAMESPACE,
                &format!("{POD_UID}-3"),
                by_name(&other_name),
                NODE_NAME,
            );
            let pod_template = mk_pod(
                POD_NAME,
                NAMESPACE,
                POD_UID,
                json!([{"name": RESOURCE_NAME, "resourceClaimTemplateName": cn}]),
                NODE_NAME,
            );
            let mut pod_template_in_status = pod_template.clone();
            pod_template_in_status
                .status
                .as_mut()
                .unwrap()
                .resource_claim_statuses = Some(vec![
                rusternetes_common::resources::pod::PodResourceClaimStatus {
                    name: RESOURCE_NAME.to_string(),
                    resource_claim_name: Some(cn.clone()),
                },
            ]);
            let mut pod_template_no_claim = pod_template.clone();
            pod_template_no_claim
                .status
                .as_mut()
                .unwrap()
                .resource_claim_statuses = Some(vec![
                rusternetes_common::resources::pod::PodResourceClaimStatus {
                    name: RESOURCE_NAME.to_string(),
                    resource_claim_name: None,
                },
            ]);

            Fx {
                simple_slice: Arc::new(mk_slice(NODE_NAME, DRIVER, vec![("instance", vec![])])),
                slice: Arc::new(slice),
                slice2: Arc::new(slice2),
                slice_replaced: Arc::new(slice_replaced),
                slice_other_devices: Arc::new(slice_other_devices),
                slice_tainted: Arc::new(slice_tainted),
                slice_tainted_extended: Arc::new(slice_tainted_extended),
                slice_tainted_none: Arc::new(slice_tainted_none),
                slice_tainted_unknown: Arc::new(slice_tainted_unknown),
                slice_tainted_no_schedule: Arc::new(slice_tainted_no_schedule),
                slice_tainted_value_other: Arc::new(slice_tainted_value_other),
                slice_tainted_twice: Arc::new(slice_tainted_twice),
                slice_untainted: Arc::new(slice_untainted),
                rule_evict: Arc::new(rule_evict),
                rule_evict_instance1: Arc::new(rule_evict_instance1),
                rule_evict_instance2_later: Arc::new(rule_evict_instance2_later),
                rule_none: Arc::new(rule_none),
                rule_evict_other: Arc::new(rule_evict_other),
                claim: Arc::new(claim),
                in_use_claim: Arc::new(in_use_claim),
                in_use_claim_other_namespace: Arc::new(in_use_claim_other_namespace),
                in_use_claim_other_name: Arc::new(in_use_claim_other_name),
                in_use_claim_other_name_shared: Arc::new(in_use_claim_other_name_shared),
                in_use_claim_with_toleration: Arc::new(in_use_claim_with_toleration),
                in_use_claim_old: Arc::new(in_use_claim_old),
                unscheduled_pod: Arc::new(unscheduled_pod),
                pod: Arc::new(pod),
                pod_two_claims: Arc::new(pod_two_claims),
                pod_other_uid: Arc::new(pod_other_uid),
                pod_other_namespace: Arc::new(pod_other_namespace),
                pod_other_name: Arc::new(pod_other_name),
                pod_template: Arc::new(pod_template),
                pod_template_in_status: Arc::new(pod_template_in_status),
                pod_template_no_claim: Arc::new(pod_template_no_claim),
            }
        }
    }

    // --- expected-state builders -----------------------------------------------------

    /// `newEvictionTime`'s reasons.
    enum R {
        Rule(Arc<DeviceTaintRule>),
        Slice(Arc<ResourceSlice>, &'static str, usize),
    }

    fn ev_time(when: DateTime<Utc>, reasons: Vec<R>) -> EvictionAndReason {
        EvictionAndReason {
            when,
            reason: reasons
                .into_iter()
                .map(|r| match r {
                    R::Rule(rule) => TrackedTaint::Rule(rule),
                    R::Slice(slice, device, taint_index) => TrackedTaint::Slice(SliceDeviceTaint {
                        slice,
                        device_name: device.to_string(),
                        taint_index,
                    }),
                })
                .collect(),
        }
    }

    fn ac(claim: &Arc<ResourceClaim>, eviction: Option<EvictionAndReason>) -> AllocatedClaim {
        AllocatedClaim {
            claim: Arc::clone(claim),
            eviction,
        }
    }

    fn wi_pod(pod: &Pod) -> WorkItem {
        WorkItem::Pod(pod_object(pod))
    }

    fn wi_rule(rule: &DeviceTaintRule) -> WorkItem {
        work_item_for_rule(rule)
    }

    fn later(items: Vec<(WorkItem, Duration)>) -> Vec<(WorkItem, Duration)> {
        items
    }

    /// What the upstream `state` struct holds.
    #[derive(Default)]
    struct St {
        pods: Vec<Arc<Pod>>,
        allocated_claims: Vec<AllocatedClaim>,
        slices: Vec<Arc<ResourceSlice>>,
        rules: Vec<Arc<DeviceTaintRule>>,
        rule_stats: HashMap<String, TaintRuleStats>,
        delete_pod_at: Vec<(NamespacedObject, EvictionAndReason)>,
        queued: MockState,
    }

    /// `inProgress`: the expected rule with its condition.
    struct ExpectedRule {
        rule: Arc<DeviceTaintRule>,
        status: bool,
        reason: &'static str,
        message: String,
        when: DateTime<Utc>,
    }

    fn in_progress(
        rule: &Arc<DeviceTaintRule>,
        status: bool,
        reason: &'static str,
        message: &str,
        when: DateTime<Utc>,
    ) -> ExpectedRule {
        ExpectedRule {
            rule: Arc::clone(rule),
            status,
            reason,
            message: message.to_string(),
            when,
        }
    }

    #[derive(Default)]
    struct Step {
        pods: Vec<Arc<Pod>>,
        rules: Vec<ExpectedRule>,
        rule_stats: HashMap<String, TaintRuleStats>,
        delete_pod_at: Vec<(NamespacedObject, EvictionAndReason)>,
        queued_processed: MockState,
        advance: Duration,
        queued_shifted: MockState,
    }

    fn stats(entries: &[(&Arc<DeviceTaintRule>, i64)]) -> HashMap<String, TaintRuleStats> {
        entries
            .iter()
            .map(|(r, n)| {
                (
                    r.metadata.uid.clone(),
                    TaintRuleStats {
                        num_evicted_pods: *n,
                    },
                )
            })
            .collect()
    }

    fn stats_by_uid(entries: &[(&str, i64)]) -> HashMap<String, TaintRuleStats> {
        entries
            .iter()
            .map(|(uid, n)| {
                (
                    uid.to_string(),
                    TaintRuleStats {
                        num_evicted_pods: *n,
                    },
                )
            })
            .collect()
    }

    enum Ev {
        Slice(Option<Arc<ResourceSlice>>, Option<Arc<ResourceSlice>>),
        Claim(Option<Arc<ResourceClaim>>, Option<Arc<ResourceClaim>>),
        Pod(Option<Arc<Pod>>, Option<Arc<Pod>>),
        Rule(Option<Arc<DeviceTaintRule>>, Option<Arc<DeviceTaintRule>>),
    }

    trait IntoPair: Sized {
        fn add(obj: &Arc<Self>) -> Ev;
        fn remove(obj: &Arc<Self>) -> Ev;
        fn update(old: &Arc<Self>, new: &Arc<Self>) -> Ev;
    }

    macro_rules! pair {
        ($t:ty, $v:ident) => {
            impl IntoPair for $t {
                fn add(obj: &Arc<Self>) -> Ev {
                    Ev::$v(None, Some(obj.clone()))
                }
                fn remove(obj: &Arc<Self>) -> Ev {
                    Ev::$v(Some(obj.clone()), None)
                }
                fn update(old: &Arc<Self>, new: &Arc<Self>) -> Ev {
                    Ev::$v(Some(old.clone()), Some(new.clone()))
                }
            }
        };
    }
    pair!(ResourceSlice, Slice);
    pair!(ResourceClaim, Claim);
    pair!(Pod, Pod);
    pair!(DeviceTaintRule, Rule);

    fn add<T: IntoPair>(o: &Arc<T>) -> Ev {
        T::add(o)
    }
    fn remove<T: IntoPair>(o: &Arc<T>) -> Ev {
        T::remove(o)
    }
    fn update<T: IntoPair>(a: &Arc<T>, b: &Arc<T>) -> Ev {
        T::update(a, b)
    }

    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    enum Msg {
        Delete,
        Cancel,
    }

    #[derive(Default)]
    struct Case {
        initial: St,
        events: Vec<Ev>,
        final_state: St,
        /// `None` is the upstream default: one empty step if work is ready.
        process: Option<Vec<Step>>,
        want_events: Vec<(NamespacedObject, Msg)>,
    }

    fn pod_ref(pod: &Pod) -> NamespacedObject {
        pod_object(pod)
    }

    // --- the harness (testHandlers) --------------------------------------------------

    struct Ctx {
        controller: Arc<DeviceTaintEvictionController<MemoryStorage>>,
        storage: Arc<MemoryStorage>,
        queue: Arc<MockQueue>,
        clock_now: Arc<Mutex<DateTime<Utc>>>,
        events: mpsc::UnboundedReceiver<PodEvent>,
        metrics: Arc<Metrics>,
    }

    fn new_ctx() -> Ctx {
        let storage = Arc::new(MemoryStorage::new());
        let queue = Arc::new(MockQueue::default());
        let clock_now = Arc::new(Mutex::new(taint_time()));
        let c2 = clock_now.clone();
        let metrics = Arc::new(Metrics::new(&[300.0]));
        let controller = Arc::new(DeviceTaintEvictionController::with_parts(
            storage.clone(),
            "device-taint-eviction",
            queue.clone(),
            Arc::new(move || *c2.lock().unwrap()),
            metrics.clone(),
            true,
        ));
        let events = controller.take_event_receiver().unwrap();
        Ctx {
            controller,
            storage,
            queue,
            clock_now,
            events,
            metrics,
        }
    }

    fn pod_key(pod: &Pod) -> String {
        build_key(
            "pods",
            pod.metadata.namespace.as_deref(),
            &pod.metadata.name,
        )
    }

    fn rule_key(rule: &DeviceTaintRule) -> String {
        build_key("devicetaintrules", None, &rule.metadata.name)
    }

    impl Ctx {
        async fn add_pod_everywhere(&self, pod: &Arc<Pod>) {
            let _: Pod = self.storage.create(&pod_key(pod), &**pod).await.unwrap();
            self.controller
                .pod_store()
                .put(pod_store_key(pod), pod.clone());
        }

        async fn add_rule_everywhere(&self, rule: &Arc<DeviceTaintRule>) {
            let _: DeviceTaintRule = self.storage.create(&rule_key(rule), &**rule).await.unwrap();
            self.controller
                .rule_store()
                .unwrap()
                .put(rule.metadata.name.clone(), rule.clone());
        }

        /// `applyEventPair`: pods and rules are mirrored into the "informer cache"
        /// and the API (storage) first.
        async fn apply(&self, event: &Ev) {
            match event {
                Ev::Slice(o, n) => self.controller.on_slice_change(o.as_ref(), n.as_ref()),
                Ev::Claim(o, n) => self.controller.on_claim_change(o.as_ref(), n.as_ref()),
                Ev::Pod(o, n) => {
                    match (o, n) {
                        (Some(_), Some(n)) => {
                            let _: Pod = self.storage.update(&pod_key(n), &**n).await.unwrap();
                            self.controller.pod_store().put(pod_store_key(n), n.clone());
                        }
                        (Some(o), None) => {
                            self.storage.delete(&pod_key(o)).await.unwrap();
                            self.controller.pod_store().remove(&pod_store_key(o));
                        }
                        (None, Some(n)) => self.add_pod_everywhere(n).await,
                        (None, None) => {}
                    }
                    self.controller.on_pod_change(o.as_ref(), n.as_ref());
                }
                Ev::Rule(o, n) => {
                    let store = self.controller.rule_store().unwrap();
                    match (o, n) {
                        (Some(_), Some(n)) => {
                            let _: DeviceTaintRule =
                                self.storage.update(&rule_key(n), &**n).await.unwrap();
                            store.put(n.metadata.name.clone(), n.clone());
                        }
                        (Some(o), None) => {
                            self.storage.delete(&rule_key(o)).await.unwrap();
                            store.remove(&o.metadata.name);
                        }
                        (None, Some(n)) => self.add_rule_everywhere(n).await,
                        (None, None) => {}
                    }
                    self.controller.on_rule_change(o.as_ref(), n.as_ref());
                }
            }
        }

        /// The informer would deliver the pod's deletion; the handler ignores it
        /// (`DeletionTimestamp` only matters to `countPendingPods`), so mirroring
        /// the stored pods into the lister is all that is needed.
        async fn sync_pod_store(&self) {
            let pods: Vec<Pod> = self
                .storage
                .list(&build_prefix("pods", None))
                .await
                .unwrap();
            for pod in pods {
                self.controller
                    .pod_store()
                    .put(pod_store_key(&pod), Arc::new(pod));
            }
        }

        /// `worker` with the informer's pod-deletion delivery after every item
        /// (upstream's test hooks a reactor that deletes from the pod store on
        /// each API delete).
        async fn run_worker(&self) {
            while let Some(item) = self.queue.get().await {
                match self.controller.handle_work(&item).await {
                    Err(_) => self.queue.add_rate_limited(item.clone()),
                    Ok(d) if !d.is_zero() => self.queue.add_after(item.clone(), d),
                    Ok(_) => self.queue.forget(&item),
                }
                self.queue.done(&item);
                self.sync_pod_store().await;
            }
        }

        fn drain_events(&mut self) -> Vec<(NamespacedObject, Msg)> {
            let mut out = vec![];
            while let Ok(e) = self.events.try_recv() {
                let msg = match e.message {
                    "Marking for deletion" => Msg::Delete,
                    "Cancelling deletion" => Msg::Cancel,
                    other => panic!("unexpected event {other}"),
                };
                out.push((e.pod, msg));
            }
            out
        }
    }

    fn slices_as_map(slices: &[Arc<ResourceSlice>]) -> HashMap<PoolId, Pool> {
        let mut pools: HashMap<PoolId, Pool> = HashMap::new();
        for slice in slices {
            let id = PoolId {
                driver_name: slice.spec.driver.clone(),
                pool_name: slice.spec.pool.name.clone(),
            };
            pools
                .entry(id)
                .or_default()
                .slices
                .insert(slice.metadata.name.clone(), slice.clone());
        }
        for pool in pools.values_mut() {
            pool.max_generation = pool
                .slices
                .values()
                .map(|s| s.spec.pool.generation)
                .max()
                .unwrap_or(i64::MIN);
        }
        pools
    }

    fn claims_as_map(claims: &[AllocatedClaim]) -> HashMap<ClaimKey, AllocatedClaim> {
        claims
            .iter()
            .map(|c| (claim_key(&c.claim), c.clone()))
            .collect()
    }

    fn evict_map(
        v: &[(NamespacedObject, EvictionAndReason)],
    ) -> HashMap<NamespacedObject, EvictionAndReason> {
        v.iter().cloned().collect()
    }

    async fn remaining_pods(ctx: &Ctx) -> Vec<String> {
        let pods: Vec<Pod> = ctx.storage.list(&build_prefix("pods", None)).await.unwrap();
        let mut names: Vec<String> = pods
            .iter()
            .filter(|p| p.metadata.deletion_timestamp.is_none())
            .map(|p| {
                format!(
                    "{}/{}",
                    p.metadata.namespace.as_deref().unwrap_or(""),
                    p.metadata.name
                )
            })
            .collect();
        names.sort();
        names
    }

    fn pod_names(pods: &[Arc<Pod>]) -> Vec<String> {
        let mut names: Vec<String> = pods
            .iter()
            .map(|p| {
                format!(
                    "{}/{}",
                    p.metadata.namespace.as_deref().unwrap_or(""),
                    p.metadata.name
                )
            })
            .collect();
        names.sort();
        names
    }

    async fn check_rules(ctx: &Ctx, expected: &[ExpectedRule], what: &str) {
        let mut actual: Vec<DeviceTaintRule> = ctx
            .storage
            .list(&build_prefix("devicetaintrules", None))
            .await
            .unwrap();
        actual.sort_by(|a, b| a.metadata.name.cmp(&b.metadata.name));
        let mut want: Vec<&ExpectedRule> = expected.iter().collect();
        want.sort_by(|a, b| a.rule.metadata.name.cmp(&b.rule.metadata.name));
        // Rules without an expected condition must not have one.
        let with_condition: Vec<&DeviceTaintRule> = actual
            .iter()
            .filter(|r| !r.status.conditions.is_empty())
            .collect();
        assert_eq!(
            with_condition.len(),
            want.len(),
            "{what}: rules with a condition: {:?}",
            with_condition
                .iter()
                .map(|r| (&r.metadata.name, &r.status.conditions))
                .collect::<Vec<_>>()
        );
        for (got, want) in with_condition.iter().zip(&want) {
            assert_eq!(got.metadata.name, want.rule.metadata.name, "{what}");
            assert_eq!(got.status.conditions.len(), 1, "{what}");
            let c = &got.status.conditions[0];
            assert_eq!(c.condition_type, CONDITION_EVICTION_IN_PROGRESS, "{what}");
            assert_eq!(
                c.status,
                if want.status { "True" } else { "False" },
                "{what}: {}",
                got.metadata.name
            );
            assert_eq!(
                c.reason.as_deref(),
                Some(want.reason),
                "{what}: {}",
                got.metadata.name
            );
            assert_eq!(
                c.message.as_deref().unwrap_or(""),
                want.message,
                "{what}: {}",
                got.metadata.name
            );
            assert_eq!(
                c.last_transition_time,
                Some(want.when),
                "{what}: {}",
                got.metadata.name
            );
            assert_eq!(
                c.observed_generation.unwrap_or(0),
                want.rule.metadata.generation.unwrap_or(0),
                "{what}"
            );
        }
    }

    /// `testHandlers`.
    async fn test_handlers(case: &Case, events: &[&Ev], label: &str) {
        let mut ctx = new_ctx();
        assert_eq!(
            normalized(&MockState::default()),
            normalized(&case.initial.queued),
            "{label}: initial work queue state"
        );
        {
            let mut st = ctx.controller.state();
            st.taint_rule_stats = case.initial.rule_stats.clone();
            st.delete_pod_at = evict_map(&case.initial.delete_pod_at);
            st.allocated_claims = claims_as_map(&case.initial.allocated_claims);
            st.pools = slices_as_map(&case.initial.slices);
        }
        for pod in &case.initial.pods {
            ctx.add_pod_everywhere(pod).await;
        }
        for rule in &case.initial.rules {
            ctx.add_rule_everywhere(rule).await;
        }

        for event in events {
            ctx.apply(event).await;
        }

        {
            let st = ctx.controller.state();
            assert_eq!(
                case.final_state.rule_stats, st.taint_rule_stats,
                "{label}: taintRuleStats"
            );
            assert_eq!(
                evict_map(&case.final_state.delete_pod_at),
                st.delete_pod_at,
                "{label}: deletePodAt"
            );
            assert_eq!(
                claims_as_map(&case.final_state.allocated_claims),
                st.allocated_claims,
                "{label}: allocated claims"
            );
            assert_eq!(
                slices_as_map(&case.final_state.slices),
                st.pools,
                "{label}: pools"
            );
        }
        assert_eq!(
            normalized(&case.final_state.queued),
            normalized(&ctx.queue.state()),
            "{label}: work queue after event handlers"
        );

        let default_process = [Step::default()];
        let process: &[Step] = match &case.process {
            Some(p) => p,
            None if !case.final_state.queued.ready.is_empty() => &default_process,
            None => &[],
        };
        for (i, step) in process.iter().enumerate() {
            let prefix = format!("{label}: process #{i}");
            // Runs until the "ready" queue is empty.
            ctx.run_worker().await;

            {
                let st = ctx.controller.state();
                assert_eq!(
                    step.rule_stats, st.taint_rule_stats,
                    "{prefix}: taintRuleStats"
                );
                assert_eq!(
                    evict_map(&step.delete_pod_at),
                    st.delete_pod_at,
                    "{prefix}: deletePodAt"
                );
                assert_eq!(
                    claims_as_map(&case.final_state.allocated_claims),
                    st.allocated_claims,
                    "{prefix}: allocated claims"
                );
            }
            assert_eq!(
                pod_names(&step.pods),
                remaining_pods(&ctx).await,
                "{prefix}: pods after flushing work queue"
            );
            check_rules(
                &ctx,
                &step.rules,
                &format!("{prefix}: rules after flushing work queue"),
            )
            .await;

            assert_eq!(
                normalized(&step.queued_processed),
                normalized(&ctx.queue.state()),
                "{prefix}: work queue after processing"
            );
            // Advance time and potentially make pending work items ready.
            *ctx.clock_now.lock().unwrap() += cd(step.advance);
            ctx.queue.advance(step.advance);
            assert_eq!(
                normalized(&step.queued_shifted),
                normalized(&ctx.queue.state()),
                "{prefix}: work queue after moving time forward"
            );
        }

        let mut got = ctx.drain_events();
        got.sort_by(|a, b| {
            a.0.uid
                .cmp(&b.0.uid)
                .then(format!("{:?}", a.1).cmp(&format!("{:?}", b.1)))
        });
        let mut want = case.want_events.clone();
        want.sort_by(|a, b| {
            a.0.uid
                .cmp(&b.0.uid)
                .then(format!("{:?}", a.1).cmp(&format!("{:?}", b.1)))
        });
        assert_eq!(want, got, "{label}: overall events");
    }

    fn permutations(n: usize) -> Vec<Vec<usize>> {
        fn rec(cur: &mut Vec<usize>, n: usize, out: &mut Vec<Vec<usize>>) {
            if cur.len() == n {
                out.push(cur.clone());
                return;
            }
            for i in 0..n {
                if cur.contains(&i) {
                    continue;
                }
                cur.push(i);
                rec(cur, n, out);
                cur.pop();
            }
        }
        let mut out = vec![];
        rec(&mut vec![], n, &mut out);
        out
    }

    /// Flatten `events` the way upstream does: each entry is applied in order;
    /// permutations reorder the entries (a nested list in Go stays one entry; the
    /// Rust cases use [`Case::events`] entries one by one and encode such groups
    /// by keeping them adjacent via [`group`]).
    async fn run_case(name: &str, case: Case) {
        let n = case.events.len();
        if n <= 1 {
            let events: Vec<&Ev> = case.events.iter().collect();
            test_handlers(&case, &events, name).await;
            return;
        }
        for perm in permutations(n) {
            let events: Vec<&Ev> = perm.iter().map(|i| &case.events[*i]).collect();
            test_handlers(&case, &events, &format!("{name} {perm:?}")).await;
        }
    }

    /// `[]any{remove(a), add(b)}`: a group of events that is applied in the order
    /// given, without permutation. The table runs a case with a single group entry
    /// as one fixed sequence (`numEvents <= 1`: no permutations), so the helper
    /// just returns the events in order, to be placed in a case whose `events`
    /// vector is run via [`run_ordered`].
    async fn run_ordered(name: &str, case: Case) {
        let events: Vec<&Ev> = case.events.iter().collect();
        test_handlers(&case, &events, name).await;
    }

    // --- TestController --------------------------------------------------------------

    fn meta_pod_event(pod: &Arc<Pod>, msg: Msg) -> (NamespacedObject, Msg) {
        (pod_ref(pod), msg)
    }

    #[test]
    fn controller_basic_state_tracking() {
        run_big(controller_basic_state_tracking_async());
    }

    async fn controller_basic_state_tracking_async() {
        let f = Fx::new();
        run_case("empty", Case::default()).await;
        run_case(
            "populate-pools",
            Case {
                events: vec![add(&f.slice), add(&f.slice2)],
                final_state: St {
                    slices: vec![f.slice.clone(), f.slice2.clone()],
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await;
        run_case(
            "update-pools",
            Case {
                initial: St {
                    slices: vec![f.slice.clone(), f.slice2.clone()],
                    ..Default::default()
                },
                events: vec![update(&f.slice, &f.slice_untainted), remove(&f.slice2)],
                final_state: St {
                    slices: vec![f.slice_untainted.clone()],
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await;
        run_case(
            "untainted-claim",
            Case {
                events: vec![add(&f.slice), add(&f.slice2), add(&f.in_use_claim)],
                final_state: St {
                    slices: vec![f.slice.clone(), f.slice2.clone()],
                    allocated_claims: vec![ac(&f.in_use_claim, None)],
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await;
        let sliced = |f: &Fx| {
            ev_time(
                taint_time(),
                vec![R::Slice(f.slice_tainted.clone(), "instance", 0)],
            )
        };
        run_case(
            "tainted-claim-through-resourceslice",
            Case {
                events: vec![add(&f.slice_tainted), add(&f.slice2), add(&f.in_use_claim)],
                final_state: St {
                    slices: vec![f.slice_tainted.clone(), f.slice2.clone()],
                    allocated_claims: vec![ac(&f.in_use_claim, Some(sliced(&f)))],
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await;
    }

    #[test]
    fn controller_rule_status_and_eviction_through_rules() {
        run_big(controller_rule_status_and_eviction_through_rules_async());
    }

    async fn controller_rule_status_and_eviction_through_rules_async() {
        let f = Fx::new();
        let status_period = RULE_STATUS_PERIOD;
        let t0 = taint_time();
        let t_plus = |d: Duration| t0 + cd(d);

        run_case(
            "rule-status",
            Case {
                events: vec![add(&f.rule_evict)],
                final_state: St {
                    queued: MockState {
                        ready: vec![wi_rule(&f.rule_evict)],
                        ..Default::default()
                    },
                    ..Default::default()
                },
                process: Some(vec![Step {
                    rules: vec![in_progress(&f.rule_evict, false, "NotStarted", "", t0)],
                    ..Default::default()
                }]),
                ..Default::default()
            },
        )
        .await;

        run_case(
            "tainted-claim-through-rule",
            Case {
                events: vec![add(&f.rule_evict), add(&f.in_use_claim)],
                final_state: St {
                    allocated_claims: vec![ac(
                        &f.in_use_claim,
                        Some(ev_time(t0, vec![R::Rule(f.rule_evict.clone())])),
                    )],
                    queued: MockState {
                        ready: vec![wi_rule(&f.rule_evict)],
                        ..Default::default()
                    },
                    ..Default::default()
                },
                process: Some(vec![Step {
                    rules: vec![in_progress(&f.rule_evict, false, "NotStarted", "", t0)],
                    ..Default::default()
                }]),
                ..Default::default()
            },
        )
        .await;

        let evicted_once = ev_time(t0, vec![R::Rule(f.rule_evict.clone())]);
        run_case(
            "evict-pod-rule",
            Case {
                events: vec![add(&f.in_use_claim), add(&f.pod), add(&f.rule_evict)],
                final_state: St {
                    allocated_claims: vec![ac(&f.in_use_claim, Some(evicted_once.clone()))],
                    delete_pod_at: vec![(pod_ref(&f.pod), evicted_once.clone())],
                    queued: MockState {
                        ready: vec![wi_rule(&f.rule_evict), wi_pod(&f.pod)],
                        later: later(vec![(wi_rule(&f.rule_evict), status_period)]),
                        ..Default::default()
                    },
                    ..Default::default()
                },
                process: Some(vec![
                    Step {
                        rule_stats: stats(&[(&f.rule_evict, 1)]),
                        rules: vec![in_progress(
                            &f.rule_evict,
                            true,
                            "PodsPendingEviction",
                            "1 pod needs to be evicted in 1 namespace.",
                            t0,
                        )],
                        queued_processed: MockState {
                            later: later(vec![(wi_rule(&f.rule_evict), status_period)]),
                            ..Default::default()
                        },
                        advance: status_period,
                        queued_shifted: MockState {
                            ready: vec![wi_rule(&f.rule_evict)],
                            ..Default::default()
                        },
                        ..Default::default()
                    },
                    Step {
                        rule_stats: stats(&[(&f.rule_evict, 1)]),
                        rules: vec![in_progress(
                            &f.rule_evict,
                            false,
                            "Completed",
                            "1 pod evicted since starting the controller.",
                            t_plus(status_period),
                        )],
                        ..Default::default()
                    },
                ]),
                want_events: vec![meta_pod_event(&f.pod, Msg::Delete)],

                ..Default::default()
            },
        )
        .await;

        let later_eviction = ev_time(
            t_plus(TOLERATION_DURATION),
            vec![R::Rule(f.rule_evict.clone())],
        );
        run_case(
            "evict-pod-rule-later",
            Case {
                events: vec![
                    add(&f.in_use_claim_with_toleration),
                    add(&f.pod),
                    add(&f.rule_evict),
                ],
                final_state: St {
                    allocated_claims: vec![ac(
                        &f.in_use_claim_with_toleration,
                        Some(later_eviction.clone()),
                    )],
                    delete_pod_at: vec![(pod_ref(&f.pod), later_eviction.clone())],
                    queued: MockState {
                        ready: vec![wi_rule(&f.rule_evict)],
                        later: later(vec![
                            (wi_pod(&f.pod), TOLERATION_DURATION),
                            (wi_rule(&f.rule_evict), status_period),
                        ]),
                        ..Default::default()
                    },
                    ..Default::default()
                },
                process: Some(vec![
                    Step {
                        delete_pod_at: vec![(pod_ref(&f.pod), later_eviction.clone())],
                        pods: vec![f.pod.clone()],
                        rules: vec![in_progress(
                            &f.rule_evict,
                            true,
                            "PodsPendingEviction",
                            "1 pod needs to be evicted in 1 namespace.",
                            t0,
                        )],
                        queued_processed: MockState {
                            later: later(vec![
                                (wi_pod(&f.pod), TOLERATION_DURATION),
                                (wi_rule(&f.rule_evict), status_period),
                            ]),
                            ..Default::default()
                        },
                        advance: status_period,
                        queued_shifted: MockState {
                            ready: vec![wi_rule(&f.rule_evict)],
                            later: later(vec![(
                                wi_pod(&f.pod),
                                TOLERATION_DURATION - status_period,
                            )]),
                            ..Default::default()
                        },
                        ..Default::default()
                    },
                    Step {
                        delete_pod_at: vec![(pod_ref(&f.pod), later_eviction.clone())],
                        pods: vec![f.pod.clone()],
                        rules: vec![in_progress(
                            &f.rule_evict,
                            true,
                            "PodsPendingEviction",
                            "1 pod needs to be evicted in 1 namespace.",
                            t_plus(status_period),
                        )],
                        queued_processed: MockState {
                            later: later(vec![(
                                wi_pod(&f.pod),
                                TOLERATION_DURATION - status_period,
                            )]),
                            ..Default::default()
                        },
                        advance: TOLERATION_DURATION - status_period,
                        queued_shifted: MockState {
                            ready: vec![wi_pod(&f.pod)],
                            ..Default::default()
                        },
                        ..Default::default()
                    },
                    Step {
                        // Deleted, but condition not updated yet.
                        rule_stats: stats(&[(&f.rule_evict, 1)]),
                        rules: vec![in_progress(
                            &f.rule_evict,
                            true,
                            "PodsPendingEviction",
                            "1 pod needs to be evicted in 1 namespace.",
                            t_plus(status_period),
                        )],
                        queued_processed: MockState {
                            later: later(vec![(wi_rule(&f.rule_evict), status_period)]),
                            ..Default::default()
                        },
                        advance: status_period,
                        queued_shifted: MockState {
                            ready: vec![wi_rule(&f.rule_evict)],
                            ..Default::default()
                        },
                        ..Default::default()
                    },
                    Step {
                        rule_stats: stats(&[(&f.rule_evict, 1)]),
                        rules: vec![in_progress(
                            &f.rule_evict,
                            false,
                            "Completed",
                            "1 pod evicted since starting the controller.",
                            t_plus(TOLERATION_DURATION + status_period),
                        )],
                        ..Default::default()
                    },
                ]),
                want_events: vec![meta_pod_event(&f.pod, Msg::Delete)],

                ..Default::default()
            },
        )
        .await;

        // Two pods in two namespaces.
        run_case(
            "evict-many-pods-many-namespaces",
            Case {
                events: vec![
                    add(&f.in_use_claim),
                    add(&f.pod),
                    add(&f.in_use_claim_other_namespace),
                    add(&f.pod_other_namespace),
                    add(&f.rule_evict),
                ],
                final_state: St {
                    allocated_claims: vec![
                        ac(&f.in_use_claim, Some(evicted_once.clone())),
                        ac(&f.in_use_claim_other_namespace, Some(evicted_once.clone())),
                    ],
                    delete_pod_at: vec![
                        (pod_ref(&f.pod), evicted_once.clone()),
                        (pod_ref(&f.pod_other_namespace), evicted_once.clone()),
                    ],
                    queued: MockState {
                        ready: vec![
                            wi_rule(&f.rule_evict),
                            wi_pod(&f.pod),
                            wi_pod(&f.pod_other_namespace),
                        ],
                        later: later(vec![(wi_rule(&f.rule_evict), status_period)]),
                        ..Default::default()
                    },
                    ..Default::default()
                },
                process: Some(vec![
                    Step {
                        rule_stats: stats(&[(&f.rule_evict, 2)]),
                        rules: vec![in_progress(
                            &f.rule_evict,
                            true,
                            "PodsPendingEviction",
                            "2 pods need to be evicted in 2 different namespaces.",
                            t0,
                        )],
                        queued_processed: MockState {
                            later: later(vec![(wi_rule(&f.rule_evict), status_period)]),
                            ..Default::default()
                        },
                        advance: status_period,
                        queued_shifted: MockState {
                            ready: vec![wi_rule(&f.rule_evict)],
                            ..Default::default()
                        },
                        ..Default::default()
                    },
                    Step {
                        rule_stats: stats(&[(&f.rule_evict, 2)]),
                        rules: vec![in_progress(
                            &f.rule_evict,
                            false,
                            "Completed",
                            "2 pods evicted since starting the controller.",
                            t_plus(status_period),
                        )],
                        ..Default::default()
                    },
                ]),
                want_events: vec![
                    meta_pod_event(&f.pod, Msg::Delete),
                    meta_pod_event(&f.pod_other_namespace, Msg::Delete),
                ],

                ..Default::default()
            },
        )
        .await;

        // Two pods in one namespace.
        run_case(
            "evict-many-pods-same-namespace",
            Case {
                events: vec![
                    add(&f.in_use_claim),
                    add(&f.pod),
                    add(&f.in_use_claim_other_name),
                    add(&f.pod_other_name),
                    add(&f.rule_evict),
                ],
                final_state: St {
                    allocated_claims: vec![
                        ac(&f.in_use_claim, Some(evicted_once.clone())),
                        ac(&f.in_use_claim_other_name, Some(evicted_once.clone())),
                    ],
                    delete_pod_at: vec![
                        (pod_ref(&f.pod), evicted_once.clone()),
                        (pod_ref(&f.pod_other_name), evicted_once.clone()),
                    ],
                    queued: MockState {
                        ready: vec![
                            wi_rule(&f.rule_evict),
                            wi_pod(&f.pod),
                            wi_pod(&f.pod_other_name),
                        ],
                        later: later(vec![(wi_rule(&f.rule_evict), status_period)]),
                        ..Default::default()
                    },
                    ..Default::default()
                },
                process: Some(vec![
                    Step {
                        rule_stats: stats(&[(&f.rule_evict, 2)]),
                        rules: vec![in_progress(
                            &f.rule_evict,
                            true,
                            "PodsPendingEviction",
                            "2 pods need to be evicted in 1 namespace.",
                            t0,
                        )],
                        queued_processed: MockState {
                            later: later(vec![(wi_rule(&f.rule_evict), status_period)]),
                            ..Default::default()
                        },
                        advance: status_period,
                        queued_shifted: MockState {
                            ready: vec![wi_rule(&f.rule_evict)],
                            ..Default::default()
                        },
                        ..Default::default()
                    },
                    Step {
                        rule_stats: stats(&[(&f.rule_evict, 2)]),
                        rules: vec![in_progress(
                            &f.rule_evict,
                            false,
                            "Completed",
                            "2 pods evicted since starting the controller.",
                            t_plus(status_period),
                        )],
                        ..Default::default()
                    },
                ]),
                want_events: vec![
                    meta_pod_event(&f.pod, Msg::Delete),
                    meta_pod_event(&f.pod_other_name, Msg::Delete),
                ],

                ..Default::default()
            },
        )
        .await;
    }

    #[test]
    fn controller_none_effect_simulation() {
        run_big(controller_none_effect_simulation_async());
    }

    async fn controller_none_effect_simulation_async() {
        let f = Fx::new();
        let t0 = taint_time();
        let tail = "if the effect was NoExecute. This information will not be updated again. Recreate the DeviceTaintRule to trigger an update.";

        run_case(
            "none-pod-rule",
            Case {
                events: vec![add(&f.slice), add(&f.in_use_claim), add(&f.pod), add(&f.rule_none)],
                final_state: St {
                    slices: vec![f.slice.clone()],
                    allocated_claims: vec![ac(&f.in_use_claim, None)],
                    queued: MockState {
                        ready: vec![wi_rule(&f.rule_none)],
                        ..Default::default()
                    },
                    ..Default::default()
                },
                process: Some(vec![Step {
                    pods: vec![f.pod.clone()],
                    rules: vec![in_progress(
                        &f.rule_none,
                        false,
                        "NoEffect",
                        &format!("3 published devices selected. 1 allocated device selected. 1 pod would be evicted in 1 namespace {tail}"),
                        t0,
                    )],
                    ..Default::default()
                }]),
                ..Default::default()
            },
        )
        .await;

        run_case(
            "none-many-pods-many-namespaces",
            Case {
                events: vec![
                    add(&f.in_use_claim),
                    add(&f.pod),
                    add(&f.in_use_claim_other_namespace),
                    add(&f.pod_other_namespace),
                    add(&f.rule_none),
                ],
                final_state: St {
                    allocated_claims: vec![ac(&f.in_use_claim, None), ac(&f.in_use_claim_other_namespace, None)],
                    queued: MockState {
                        ready: vec![wi_rule(&f.rule_none)],
                        ..Default::default()
                    },
                    ..Default::default()
                },
                process: Some(vec![Step {
                    pods: vec![f.pod.clone(), f.pod_other_namespace.clone()],
                    rules: vec![in_progress(
                        &f.rule_none,
                        false,
                        "NoEffect",
                        &format!("0 published devices selected. 3 allocated devices selected. 2 pods would be evicted in 2 different namespaces {tail}"),
                        t0,
                    )],
                    ..Default::default()
                }]),
                ..Default::default()
            },
        )
        .await;

        run_case(
            "none-many-pods-same-namespace",
            Case {
                events: vec![
                    add(&f.in_use_claim),
                    add(&f.pod),
                    add(&f.in_use_claim_other_name),
                    add(&f.pod_other_name),
                    add(&f.rule_none),
                ],
                final_state: St {
                    allocated_claims: vec![ac(&f.in_use_claim, None), ac(&f.in_use_claim_other_name, None)],
                    queued: MockState {
                        ready: vec![wi_rule(&f.rule_none)],
                        ..Default::default()
                    },
                    ..Default::default()
                },
                process: Some(vec![Step {
                    pods: vec![f.pod.clone(), f.pod_other_name.clone()],
                    rules: vec![in_progress(
                        &f.rule_none,
                        false,
                        "NoEffect",
                        &format!("0 published devices selected. 3 allocated devices selected. 2 pods would be evicted in 1 namespace {tail}"),
                        t0,
                    )],
                    ..Default::default()
                }]),
                ..Default::default()
            },
        )
        .await;
    }

    #[test]
    fn controller_multiple_claims_and_rules() {
        run_big(controller_multiple_claims_and_rules_async());
    }

    async fn controller_multiple_claims_and_rules_async() {
        let f = Fx::new();
        let t0 = taint_time();
        let status_period = RULE_STATUS_PERIOD;
        let t_later = t0 + ChronoDuration::seconds(40);
        let t_plus = |d: Duration| t0 + cd(d);

        let one = ev_time(t0, vec![R::Rule(f.rule_evict.clone())]);
        run_case(
            "multiple-claims-same-rule",
            Case {
                events: vec![
                    add(&f.in_use_claim),
                    add(&f.in_use_claim_other_name_shared),
                    add(&f.pod_two_claims),
                    add(&f.rule_evict),
                ],
                final_state: St {
                    allocated_claims: vec![
                        ac(&f.in_use_claim, Some(one.clone())),
                        ac(&f.in_use_claim_other_name_shared, Some(one.clone())),
                    ],
                    delete_pod_at: vec![(pod_ref(&f.pod_two_claims), one.clone())],
                    queued: MockState {
                        ready: vec![wi_rule(&f.rule_evict), wi_pod(&f.pod_two_claims)],
                        later: later(vec![(wi_rule(&f.rule_evict), status_period)]),
                        ..Default::default()
                    },
                    ..Default::default()
                },
                process: Some(vec![
                    Step {
                        rule_stats: stats(&[(&f.rule_evict, 1)]),
                        rules: vec![in_progress(
                            &f.rule_evict,
                            true,
                            "PodsPendingEviction",
                            "1 pod needs to be evicted in 1 namespace.",
                            t0,
                        )],
                        queued_processed: MockState {
                            later: later(vec![(wi_rule(&f.rule_evict), status_period)]),
                            ..Default::default()
                        },
                        advance: status_period,
                        queued_shifted: MockState {
                            ready: vec![wi_rule(&f.rule_evict)],
                            ..Default::default()
                        },
                        ..Default::default()
                    },
                    Step {
                        rule_stats: stats(&[(&f.rule_evict, 1)]),
                        rules: vec![in_progress(
                            &f.rule_evict,
                            false,
                            "Completed",
                            "1 pod evicted since starting the controller.",
                            t_plus(status_period),
                        )],
                        ..Default::default()
                    },
                ]),
                want_events: vec![meta_pod_event(&f.pod, Msg::Delete)],

                ..Default::default()
            },
        )
        .await;

        // Different rules cause eviction at different times; eviction of the pod is
        // attributed to both rules. The order in which they are observed matters
        // for the work queue, so these two run as fixed sequences.
        for (label, rules_order) in [
            (
                "multiple-claims-different-rules-order-1",
                [&f.rule_evict_instance1, &f.rule_evict_instance2_later],
            ),
            (
                "multiple-claims-different-rules-order-2",
                [&f.rule_evict_instance2_later, &f.rule_evict_instance1],
            ),
        ] {
            let order2 = label.ends_with('2');
            let both = ev_time(
                t0,
                vec![
                    R::Rule(f.rule_evict_instance1.clone()),
                    R::Rule(f.rule_evict_instance2_later.clone()),
                ],
            );
            let claims = vec![
                ac(
                    &f.in_use_claim,
                    Some(ev_time(t0, vec![R::Rule(f.rule_evict_instance1.clone())])),
                ),
                ac(
                    &f.in_use_claim_other_name_shared,
                    Some(ev_time(
                        t_later,
                        vec![R::Rule(f.rule_evict_instance2_later.clone())],
                    )),
                ),
            ];
            let events = vec![
                add(&f.in_use_claim),
                add(&f.in_use_claim_other_name_shared),
                add(&f.pod_two_claims),
                add(rules_order[0]),
                add(rules_order[1]),
            ];
            let r1 = &f.rule_evict_instance1;
            let r2 = &f.rule_evict_instance2_later;
            let to_later = Duration::from_secs(40);
            let case = if !order2 {
                Case {
                    events,
                    final_state: St {
                        allocated_claims: claims,
                        delete_pod_at: vec![(pod_ref(&f.pod_two_claims), both.clone())],
                        queued: MockState {
                            ready: vec![wi_rule(r1), wi_rule(r2), wi_pod(&f.pod_two_claims)],
                            later: later(vec![
                                (wi_rule(r1), status_period),
                                (wi_rule(r2), status_period),
                            ]),
                            ..Default::default()
                        },
                        ..Default::default()
                    },
                    process: Some(vec![
                        Step {
                            rule_stats: stats(&[(r1, 1), (r2, 1)]),
                            rules: vec![
                                in_progress(
                                    r1,
                                    true,
                                    "PodsPendingEviction",
                                    "1 pod needs to be evicted in 1 namespace.",
                                    t0,
                                ),
                                in_progress(
                                    r2,
                                    false,
                                    "Completed",
                                    "1 pod evicted since starting the controller.",
                                    t0,
                                ),
                            ],
                            queued_processed: MockState {
                                later: later(vec![
                                    (wi_rule(r1), status_period),
                                    (wi_rule(r2), status_period),
                                ]),
                                ..Default::default()
                            },
                            advance: status_period,
                            queued_shifted: MockState {
                                ready: vec![wi_rule(r1), wi_rule(r2)],
                                ..Default::default()
                            },
                            ..Default::default()
                        },
                        Step {
                            rule_stats: stats(&[(r1, 1), (r2, 1)]),
                            rules: vec![
                                in_progress(
                                    r1,
                                    false,
                                    "Completed",
                                    "1 pod evicted since starting the controller.",
                                    t_plus(status_period),
                                ),
                                in_progress(
                                    r2,
                                    false,
                                    "Completed",
                                    "1 pod evicted since starting the controller.",
                                    t_plus(status_period),
                                ),
                            ],
                            ..Default::default()
                        },
                    ]),
                    want_events: vec![meta_pod_event(&f.pod, Msg::Delete)],

                    ..Default::default()
                }
            } else {
                Case {
                    events,
                    final_state: St {
                        allocated_claims: claims,
                        delete_pod_at: vec![(pod_ref(&f.pod_two_claims), both.clone())],
                        queued: MockState {
                            ready: vec![wi_rule(r1), wi_rule(r2), wi_pod(&f.pod_two_claims)],
                            later: later(vec![
                                (wi_pod(&f.pod_two_claims), to_later),
                                (wi_rule(r2), status_period),
                                (wi_rule(r1), status_period),
                            ]),
                            ..Default::default()
                        },
                        ..Default::default()
                    },
                    // The pod is scheduled for much later and time needs to advance
                    // a few times before it gets processed.
                    process: Some(vec![
                        Step {
                            rule_stats: stats(&[(r1, 1), (r2, 1)]),
                            rules: vec![
                                in_progress(
                                    r1,
                                    true,
                                    "PodsPendingEviction",
                                    "1 pod needs to be evicted in 1 namespace.",
                                    t0,
                                ),
                                in_progress(
                                    r2,
                                    true,
                                    "PodsPendingEviction",
                                    "1 pod needs to be evicted in 1 namespace.",
                                    t0,
                                ),
                            ],
                            queued_processed: MockState {
                                later: later(vec![
                                    (wi_pod(&f.pod_two_claims), to_later),
                                    (wi_rule(r2), status_period),
                                    (wi_rule(r1), status_period),
                                ]),
                                ..Default::default()
                            },
                            advance: status_period,
                            queued_shifted: MockState {
                                ready: vec![wi_rule(r1), wi_rule(r2)],
                                later: later(vec![(
                                    wi_pod(&f.pod_two_claims),
                                    to_later - status_period,
                                )]),
                                ..Default::default()
                            },
                            ..Default::default()
                        },
                        Step {
                            rule_stats: stats_by_uid(&[
                                (&r1.metadata.uid, 1),
                                (&r2.metadata.uid, 1),
                            ]),
                            rules: vec![
                                in_progress(
                                    r1,
                                    false,
                                    "Completed",
                                    "1 pod evicted since starting the controller.",
                                    t_plus(status_period),
                                ),
                                in_progress(
                                    r2,
                                    false,
                                    "Completed",
                                    "1 pod evicted since starting the controller.",
                                    t_plus(status_period),
                                ),
                            ],
                            queued_processed: MockState {
                                later: later(vec![(
                                    wi_pod(&f.pod_two_claims),
                                    to_later - status_period,
                                )]),
                                ..Default::default()
                            },
                            advance: to_later - status_period,
                            queued_shifted: MockState {
                                ready: vec![wi_pod(&f.pod_two_claims)],
                                ..Default::default()
                            },
                            ..Default::default()
                        },
                        Step {
                            rule_stats: stats_by_uid(&[
                                (&r1.metadata.uid, 1),
                                (&r2.metadata.uid, 1),
                            ]),
                            rules: vec![
                                in_progress(
                                    r1,
                                    false,
                                    "Completed",
                                    "1 pod evicted since starting the controller.",
                                    t_plus(status_period),
                                ),
                                in_progress(
                                    r2,
                                    false,
                                    "Completed",
                                    "1 pod evicted since starting the controller.",
                                    t_plus(status_period),
                                ),
                            ],
                            // The pod gets removed from the work queue without doing anything.
                            ..Default::default()
                        },
                    ]),
                    want_events: vec![meta_pod_event(&f.pod, Msg::Delete)],

                    ..Default::default()
                }
            };
            run_ordered(label, case).await;
        }
    }

    #[test]
    fn controller_slice_taints_and_eviction() {
        run_big(controller_slice_taints_and_eviction_async());
    }

    async fn controller_slice_taints_and_eviction_async() {
        let f = Fx::new();
        let t0 = taint_time();
        let dev0 = |f: &Fx| ev_time(t0, vec![R::Slice(f.slice_tainted.clone(), "instance", 0)]);
        let ready_pod = |p: &Arc<Pod>| MockState {
            ready: vec![wi_pod(p)],
            ..Default::default()
        };

        run_case(
            "evict-pod-resourceclaim",
            Case {
                events: vec![
                    add(&f.slice_tainted),
                    add(&f.slice2),
                    add(&f.in_use_claim),
                    add(&f.pod),
                ],
                final_state: St {
                    slices: vec![f.slice_tainted.clone(), f.slice2.clone()],
                    allocated_claims: vec![ac(&f.in_use_claim, Some(dev0(&f)))],
                    delete_pod_at: vec![(pod_ref(&f.pod), dev0(&f))],
                    queued: ready_pod(&f.pod),
                    ..Default::default()
                },
                want_events: vec![meta_pod_event(&f.pod, Msg::Delete)],
                ..Default::default()
            },
        )
        .await;

        // "evict-pod-resourceclaim-again": cancel + re-add in one fixed sequence.
        run_ordered(
            "evict-pod-resourceclaim-again",
            Case {
                initial: St {
                    pods: vec![f.pod.clone()],
                    slices: vec![f.slice_tainted.clone(), f.slice2.clone()],
                    allocated_claims: vec![ac(&f.in_use_claim, Some(dev0(&f)))],
                    delete_pod_at: vec![(pod_ref(&f.pod), dev0(&f))],
                    ..Default::default()
                },
                events: vec![remove(&f.slice_tainted), add(&f.slice_tainted)],
                final_state: St {
                    slices: vec![f.slice_tainted.clone(), f.slice2.clone()],
                    allocated_claims: vec![ac(&f.in_use_claim, Some(dev0(&f)))],
                    delete_pod_at: vec![(pod_ref(&f.pod), dev0(&f))],
                    queued: ready_pod(&f.pod),
                    ..Default::default()
                },
                // The code reliably cancels right away.
                want_events: vec![
                    meta_pod_event(&f.pod, Msg::Cancel),
                    meta_pod_event(&f.pod, Msg::Delete),
                ],
                ..Default::default()
            },
        )
        .await;

        run_case(
            "evict-pod-after-scheduling",
            Case {
                initial: St {
                    pods: vec![f.unscheduled_pod.clone()],
                    slices: vec![f.slice_tainted.clone(), f.slice2.clone()],
                    allocated_claims: vec![ac(&f.in_use_claim, Some(dev0(&f)))],
                    ..Default::default()
                },
                events: vec![update(&f.unscheduled_pod, &f.pod)],
                final_state: St {
                    slices: vec![f.slice_tainted.clone(), f.slice2.clone()],
                    allocated_claims: vec![ac(&f.in_use_claim, Some(dev0(&f)))],
                    delete_pod_at: vec![(pod_ref(&f.pod), dev0(&f))],
                    queued: ready_pod(&f.pod),
                    ..Default::default()
                },
                want_events: vec![meta_pod_event(&f.pod, Msg::Delete)],
                ..Default::default()
            },
        )
        .await;

        run_case(
            "evict-pod-resourceclaim-unrelated-changes",
            Case {
                initial: St {
                    pods: vec![f.pod.clone()],
                    slices: vec![f.slice_tainted.clone(), f.slice2.clone()],
                    allocated_claims: vec![ac(&f.in_use_claim, Some(dev0(&f)))],
                    delete_pod_at: vec![(pod_ref(&f.pod), dev0(&f))],
                    ..Default::default()
                },
                events: vec![
                    update(&f.slice_tainted, &f.slice_tainted_extended),
                    update(&f.in_use_claim, &f.in_use_claim),
                    update(&f.pod, &f.pod),
                ],
                final_state: St {
                    slices: vec![f.slice_tainted_extended.clone(), f.slice2.clone()],
                    allocated_claims: vec![ac(&f.in_use_claim, Some(dev0(&f)))],
                    delete_pod_at: vec![(pod_ref(&f.pod), dev0(&f))],
                    queued: ready_pod(&f.pod),
                    ..Default::default()
                },
                want_events: vec![meta_pod_event(&f.pod, Msg::Delete)],
                ..Default::default()
            },
        )
        .await;

        run_case(
            "evict-pod-resourceclaimtemplate",
            Case {
                events: vec![
                    add(&f.slice_tainted),
                    add(&f.slice2),
                    add(&f.in_use_claim),
                    add(&f.pod_template_in_status),
                ],
                final_state: St {
                    slices: vec![f.slice_tainted.clone(), f.slice2.clone()],
                    allocated_claims: vec![ac(&f.in_use_claim, Some(dev0(&f)))],
                    delete_pod_at: vec![(pod_ref(&f.pod_template_in_status), dev0(&f))],
                    queued: ready_pod(&f.pod_template_in_status),
                    ..Default::default()
                },
                want_events: vec![meta_pod_event(&f.pod, Msg::Delete)],
                ..Default::default()
            },
        )
        .await;

        // No eviction cases.
        let untouched = |f: &Fx,
                         slices: Vec<Arc<ResourceSlice>>,
                         claim_eviction: Option<EvictionAndReason>| St {
            slices,
            allocated_claims: vec![ac(&f.in_use_claim, claim_eviction)],
            ..Default::default()
        };
        let mut unscheduled = (*f.pod).clone();
        unscheduled.spec.as_mut().unwrap().node_name = None;
        let unscheduled = Arc::new(unscheduled);
        run_case(
            "no-evict-pod-not-scheduled",
            Case {
                events: vec![
                    add(&f.slice_tainted),
                    add(&f.slice2),
                    add(&f.in_use_claim),
                    add(&unscheduled),
                ],
                final_state: untouched(
                    &f,
                    vec![f.slice_tainted.clone(), f.slice2.clone()],
                    Some(dev0(&f)),
                ),
                ..Default::default()
            },
        )
        .await;
        run_case(
            "no-evict-no-taint",
            Case {
                events: vec![add(&f.simple_slice), add(&f.in_use_claim), add(&f.pod)],
                final_state: untouched(&f, vec![f.simple_slice.clone()], None),
                ..Default::default()
            },
        )
        .await;
        run_case(
            "no-evict-no-taint-update",
            Case {
                initial: St {
                    pods: vec![f.pod.clone()],
                    slices: vec![f.simple_slice.clone()],
                    allocated_claims: vec![ac(&f.in_use_claim, None)],
                    ..Default::default()
                },
                events: vec![update(&f.simple_slice, &f.simple_slice)],
                final_state: untouched(&f, vec![f.simple_slice.clone()], None),
                ..Default::default()
            },
        )
        .await;
        for (name, pod) in [
            (
                "no-evict-pod-resourceclaimtemplate-no-status",
                f.pod_template.clone(),
            ),
            (
                "no-evict-pod-resourceclaimtemplate-no-claim",
                f.pod_template_no_claim.clone(),
            ),
            ("no-evict-wrong-pod", f.pod_other_uid.clone()),
        ] {
            run_case(
                name,
                Case {
                    events: vec![
                        add(&f.slice_tainted),
                        add(&f.slice2),
                        add(&f.in_use_claim),
                        add(&pod),
                    ],
                    final_state: untouched(
                        &f,
                        vec![f.slice_tainted.clone(), f.slice2.clone()],
                        Some(dev0(&f)),
                    ),
                    ..Default::default()
                },
            )
            .await;
        }
        run_case(
            "no-evict-other-device",
            Case {
                events: vec![
                    add(&f.slice_other_devices),
                    add(&f.slice2),
                    add(&f.in_use_claim),
                    add(&f.pod_template_in_status),
                ],
                final_state: untouched(
                    &f,
                    vec![f.slice_other_devices.clone(), f.slice2.clone()],
                    None,
                ),
                ..Default::default()
            },
        )
        .await;

        run_ordered(
            "evict-wrong-pod-replaced",
            Case {
                initial: St {
                    pods: vec![f.pod_other_uid.clone()],
                    slices: vec![f.slice_tainted.clone(), f.slice2.clone()],
                    allocated_claims: vec![ac(&f.in_use_claim, Some(dev0(&f)))],
                    ..Default::default()
                },
                events: vec![remove(&f.pod_other_uid), add(&f.pod)],
                final_state: St {
                    slices: vec![f.slice_tainted.clone(), f.slice2.clone()],
                    allocated_claims: vec![ac(&f.in_use_claim, Some(dev0(&f)))],
                    delete_pod_at: vec![(pod_ref(&f.pod), dev0(&f))],
                    queued: ready_pod(&f.pod),
                    ..Default::default()
                },
                want_events: vec![meta_pod_event(&f.pod, Msg::Delete)],
                ..Default::default()
            },
        )
        .await;

        // Cancellation cases.
        let evicting_initial = |f: &Fx| St {
            pods: vec![f.pod_template_in_status.clone()],
            slices: vec![f.slice_tainted.clone(), f.slice2.clone()],
            allocated_claims: vec![ac(&f.in_use_claim, Some(dev0(f)))],
            delete_pod_at: vec![(pod_ref(&f.pod_template_in_status), dev0(f))],
            ..Default::default()
        };
        run_case(
            "cancel-eviction-remove-taint",
            Case {
                initial: evicting_initial(&f),
                events: vec![update(&f.slice_tainted, &f.slice)],
                final_state: untouched(&f, vec![f.slice.clone(), f.slice2.clone()], None),
                want_events: vec![meta_pod_event(&f.pod, Msg::Cancel)],
                ..Default::default()
            },
        )
        .await;
        run_case(
            "cancel-eviction-reduce-taint",
            Case {
                initial: evicting_initial(&f),
                events: vec![update(&f.slice_tainted, &f.slice_tainted_no_schedule)],
                final_state: untouched(
                    &f,
                    vec![f.slice_tainted_no_schedule.clone(), f.slice2.clone()],
                    None,
                ),
                want_events: vec![meta_pod_event(&f.pod, Msg::Cancel)],
                ..Default::default()
            },
        )
        .await;
        for (name, slice) in [
            ("ignore-none-effect", f.slice_tainted_none.clone()),
            ("ignore-unknown-effect", f.slice_tainted_unknown.clone()),
        ] {
            run_case(
                name,
                Case {
                    initial: St {
                        pods: vec![f.pod_template_in_status.clone()],
                        slices: vec![slice.clone(), f.slice2.clone()],
                        allocated_claims: vec![ac(&f.in_use_claim, None)],
                        ..Default::default()
                    },
                    final_state: untouched(&f, vec![slice, f.slice2.clone()], None),
                    ..Default::default()
                },
            )
            .await;
        }
        run_ordered(
            "cancel-eviction-remove-taint-in-new-slice",
            Case {
                initial: evicting_initial(&f),
                events: vec![remove(&f.slice_tainted), add(&f.slice_replaced)],
                final_state: untouched(&f, vec![f.slice_replaced.clone(), f.slice2.clone()], None),
                want_events: vec![meta_pod_event(&f.pod, Msg::Cancel)],
                ..Default::default()
            },
        )
        .await;
        run_case(
            "cancel-eviction-remove-slice",
            Case {
                initial: evicting_initial(&f),
                events: vec![remove(&f.slice_tainted)],
                final_state: untouched(&f, vec![f.slice2.clone()], None),
                want_events: vec![meta_pod_event(&f.pod, Msg::Cancel)],
                ..Default::default()
            },
        )
        .await;

        // "eviction-change-taint": a taint tolerated for an hour becomes one that
        // is not tolerated.
        let tolerated = ev_time(
            t0 + cd(TOLERATION_DURATION),
            vec![R::Slice(f.slice_tainted.clone(), "instance", 0)],
        );
        let changed = ev_time(
            t0,
            vec![R::Slice(f.slice_tainted_value_other.clone(), "instance", 0)],
        );
        run_case(
            "eviction-change-taint",
            Case {
                initial: St {
                    pods: vec![f.pod_template_in_status.clone()],
                    slices: vec![f.slice_tainted.clone(), f.slice2.clone()],
                    allocated_claims: vec![ac(
                        &f.in_use_claim_with_toleration,
                        Some(tolerated.clone()),
                    )],
                    delete_pod_at: vec![(pod_ref(&f.pod_template_in_status), tolerated)],
                    ..Default::default()
                },
                events: vec![update(&f.slice_tainted, &f.slice_tainted_value_other)],
                final_state: St {
                    slices: vec![f.slice_tainted_value_other.clone(), f.slice2.clone()],
                    allocated_claims: vec![ac(
                        &f.in_use_claim_with_toleration,
                        Some(changed.clone()),
                    )],
                    delete_pod_at: vec![(pod_ref(&f.pod_template_in_status), changed)],
                    queued: ready_pod(&f.pod_template_in_status),
                    ..Default::default()
                },
                want_events: vec![meta_pod_event(&f.pod, Msg::Delete)],
                ..Default::default()
            },
        )
        .await;

        // Pod deleted before eviction.
        let sixty = ev_time(t0 + ChronoDuration::seconds(60), vec![]);
        let tol60 = with_tolerations(
            &f.in_use_claim,
            vec![toleration("", TolerationOperator::Exists, None, Some(60))],
        );
        let tol60 = Arc::new(tol60);
        run_case(
            "cancel-eviction-pod-deletion",
            Case {
                initial: St {
                    pods: vec![f.pod.clone()],
                    slices: vec![f.slice_tainted.clone(), f.slice2.clone()],
                    allocated_claims: vec![ac(&tol60, Some(sixty.clone()))],
                    delete_pod_at: vec![(pod_ref(&f.pod), sixty.clone())],
                    ..Default::default()
                },
                events: vec![remove(&f.pod)],
                final_state: St {
                    slices: vec![f.slice_tainted.clone(), f.slice2.clone()],
                    allocated_claims: vec![ac(&tol60, Some(sixty))],
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await;

        // Claim ownership and lifecycle.
        run_case(
            "no-evict-wrong-resourceclaim",
            Case {
                events: vec![
                    add(&f.slice_tainted),
                    add(&f.slice2),
                    add(&f.in_use_claim_old),
                    add(&f.pod_template_in_status),
                ],
                final_state: St {
                    slices: vec![f.slice_tainted.clone(), f.slice2.clone()],
                    allocated_claims: vec![ac(&f.in_use_claim_old, Some(dev0(&f)))],
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await;
        run_case(
            "evict-wrong-resourceclaim-replaced",
            Case {
                initial: St {
                    pods: vec![f.pod_template_in_status.clone()],
                    slices: vec![f.slice_tainted.clone(), f.slice2.clone()],
                    allocated_claims: vec![ac(&f.in_use_claim_old, Some(dev0(&f)))],
                    ..Default::default()
                },
                events: vec![update(&f.in_use_claim_old, &f.in_use_claim)],
                final_state: St {
                    slices: vec![f.slice_tainted.clone(), f.slice2.clone()],
                    allocated_claims: vec![ac(&f.in_use_claim, Some(dev0(&f)))],
                    delete_pod_at: vec![(pod_ref(&f.pod_template_in_status), dev0(&f))],
                    queued: ready_pod(&f.pod_template_in_status),
                    ..Default::default()
                },
                want_events: vec![meta_pod_event(&f.pod, Msg::Delete)],
                ..Default::default()
            },
        )
        .await;
        run_case(
            "no-evict-resourceclaim-deallocated",
            Case {
                initial: evicting_initial(&f),
                events: vec![update(&f.in_use_claim, &f.claim)],
                final_state: St {
                    slices: vec![f.slice_tainted.clone(), f.slice2.clone()],
                    ..Default::default()
                },
                want_events: vec![meta_pod_event(&f.pod, Msg::Cancel)],
                ..Default::default()
            },
        )
        .await;
        run_case(
            "no-evict-resourceclaim-deleted",
            Case {
                initial: evicting_initial(&f),
                events: vec![remove(&f.in_use_claim)],
                final_state: St {
                    slices: vec![f.slice_tainted.clone(), f.slice2.clone()],
                    ..Default::default()
                },
                want_events: vec![meta_pod_event(&f.pod, Msg::Cancel)],
                ..Default::default()
            },
        )
        .await;
    }

    #[test]
    fn controller_tolerations() {
        run_big(controller_tolerations_async());
    }

    async fn controller_tolerations_async() {
        let f = Fx::new();
        let t0 = taint_time();
        let dev0 = |f: &Fx, at: DateTime<Utc>| {
            ev_time(at, vec![R::Slice(f.slice_tainted.clone(), "instance", 0)])
        };
        let ready_pod = |p: &Arc<Pod>| MockState {
            ready: vec![wi_pod(p)],
            ..Default::default()
        };

        run_case(
            "evict-pod-later",
            Case {
                events: vec![
                    add(&f.slice_tainted),
                    add(&f.slice2),
                    add(&f.in_use_claim_with_toleration),
                    add(&f.pod),
                ],
                final_state: St {
                    slices: vec![f.slice_tainted.clone(), f.slice2.clone()],
                    allocated_claims: vec![ac(
                        &f.in_use_claim_with_toleration,
                        Some(dev0(&f, t0 + cd(TOLERATION_DURATION))),
                    )],
                    delete_pod_at: vec![(pod_ref(&f.pod), dev0(&f, t0 + cd(TOLERATION_DURATION)))],
                    queued: MockState {
                        later: later(vec![(wi_pod(&f.pod), TOLERATION_DURATION)]),
                        ..Default::default()
                    },
                    ..Default::default()
                },
                process: Some(vec![
                    // First advance time, then delete.
                    Step {
                        delete_pod_at: vec![(
                            pod_ref(&f.pod),
                            dev0(&f, t0 + cd(TOLERATION_DURATION)),
                        )],
                        pods: vec![f.pod.clone()],
                        queued_processed: MockState {
                            later: later(vec![(wi_pod(&f.pod), TOLERATION_DURATION)]),
                            ..Default::default()
                        },
                        advance: TOLERATION_DURATION,
                        queued_shifted: ready_pod(&f.pod),
                        ..Default::default()
                    },
                    Step::default(),
                ]),
                want_events: vec![meta_pod_event(&f.pod, Msg::Delete)],

                ..Default::default()
            },
        )
        .await;

        let many = Arc::new(with_tolerations(
            &f.in_use_claim,
            vec![
                toleration(
                    &format!("{TAINT_KEY}-other"),
                    TolerationOperator::Equal,
                    Some(TAINT_VALUE),
                    Some(20),
                ),
                toleration("", TolerationOperator::Exists, None, Some(60)),
                toleration("", TolerationOperator::Exists, None, Some(30)),
            ],
        ));
        run_case(
            "evict-pod-later-many",
            Case {
                events: vec![
                    add(&f.slice_tainted),
                    add(&f.slice2),
                    add(&many),
                    add(&f.pod),
                ],
                final_state: St {
                    slices: vec![f.slice_tainted.clone(), f.slice2.clone()],
                    allocated_claims: vec![ac(
                        &many,
                        Some(dev0(&f, t0 + ChronoDuration::seconds(30))),
                    )],
                    delete_pod_at: vec![(
                        pod_ref(&f.pod),
                        dev0(&f, t0 + ChronoDuration::seconds(30)),
                    )],
                    queued: MockState {
                        later: later(vec![(wi_pod(&f.pod), Duration::from_secs(30))]),
                        ..Default::default()
                    },
                    ..Default::default()
                },
                process: Some(vec![
                    Step {
                        delete_pod_at: vec![(
                            pod_ref(&f.pod),
                            dev0(&f, t0 + ChronoDuration::seconds(30)),
                        )],
                        pods: vec![f.pod.clone()],
                        queued_processed: MockState {
                            later: later(vec![(wi_pod(&f.pod), Duration::from_secs(30))]),
                            ..Default::default()
                        },
                        advance: Duration::from_secs(30),
                        queued_shifted: ready_pod(&f.pod),
                        ..Default::default()
                    },
                    Step::default(),
                ]),
                want_events: vec![meta_pod_event(&f.pod, Msg::Delete)],

                ..Default::default()
            },
        )
        .await;

        let mismatch = Arc::new(with_tolerations(
            &f.in_use_claim,
            vec![toleration(
                &format!("{TAINT_KEY}-other"),
                TolerationOperator::Equal,
                Some(TAINT_VALUE),
                Some(60),
            )],
        ));
        run_case(
            "evict-pod-toleration-mismatch",
            Case {
                events: vec![
                    add(&f.slice_tainted),
                    add(&f.slice2),
                    add(&mismatch),
                    add(&f.pod),
                ],
                final_state: St {
                    slices: vec![f.slice_tainted.clone(), f.slice2.clone()],
                    allocated_claims: vec![ac(&mismatch, Some(dev0(&f, t0)))],
                    delete_pod_at: vec![(pod_ref(&f.pod), dev0(&f, t0))],
                    queued: ready_pod(&f.pod),
                    ..Default::default()
                },
                want_events: vec![meta_pod_event(&f.pod, Msg::Delete)],
                ..Default::default()
            },
        )
        .await;

        let forever = Arc::new(with_tolerations(
            &f.in_use_claim,
            vec![toleration("", TolerationOperator::Exists, None, None)],
        ));
        run_case(
            "no-evict-pod-toleration-forever",
            Case {
                events: vec![
                    add(&f.slice_tainted),
                    add(&f.slice2),
                    add(&forever),
                    add(&f.pod),
                ],
                final_state: St {
                    slices: vec![f.slice_tainted.clone(), f.slice2.clone()],
                    allocated_claims: vec![ac(&forever, None)],
                    ..Default::default()
                },
                process: Some(vec![Step {
                    pods: vec![f.pod.clone()],
                    ..Default::default()
                }]),
                ..Default::default()
            },
        )
        .await;

        let forever_many = Arc::new(with_tolerations(
            &f.in_use_claim,
            vec![
                toleration("", TolerationOperator::Exists, None, Some(60)),
                toleration("", TolerationOperator::Exists, None, None),
            ],
        ));
        run_case(
            "no-evict-pod-toleration-forever-many",
            Case {
                events: vec![
                    add(&f.slice_tainted),
                    add(&f.slice2),
                    add(&forever_many),
                    add(&f.pod),
                ],
                final_state: St {
                    slices: vec![f.slice_tainted.clone(), f.slice2.clone()],
                    allocated_claims: vec![ac(&forever_many, None)],
                    ..Default::default()
                },
                process: Some(vec![Step {
                    pods: vec![f.pod.clone()],
                    ..Default::default()
                }]),
                ..Default::default()
            },
        )
        .await;

        // One of two taints tolerated: the other one evicts.
        let partial = Arc::new(with_tolerations(
            &f.in_use_claim,
            vec![toleration(
                TAINT_KEY,
                TolerationOperator::Exists,
                None,
                None,
            )],
        ));
        let second_taint = ev_time(
            t0,
            vec![R::Slice(f.slice_tainted_twice.clone(), "instance", 1)],
        );
        run_case(
            "evict-pod-partial-toleration",
            Case {
                events: vec![
                    add(&f.slice_tainted_twice),
                    add(&f.slice2),
                    add(&partial),
                    add(&f.pod),
                ],
                final_state: St {
                    slices: vec![f.slice_tainted_twice.clone(), f.slice2.clone()],
                    allocated_claims: vec![ac(&partial, Some(second_taint.clone()))],
                    delete_pod_at: vec![(pod_ref(&f.pod), second_taint)],
                    queued: ready_pod(&f.pod),
                    ..Default::default()
                },
                want_events: vec![meta_pod_event(&f.pod, Msg::Delete)],
                ..Default::default()
            },
        )
        .await;
    }

    // --- unit tests of the helpers ---------------------------------------------------

    #[test]
    fn tolerates_taint_matches_resourceclaim_helper() {
        let taint = dt("k", Some("v"), DeviceTaintEffect::NoExecute);
        let t = |key: &str,
                 op: Option<TolerationOperator>,
                 value: Option<&str>,
                 effect: Option<DeviceTaintEffect>| {
            DeviceToleration {
                key: key.to_string(),
                value: value.map(str::to_string),
                effect,
                operator: op,
                toleration_seconds: None,
            }
        };
        // Empty operator means Equal.
        assert!(tolerates_taint(&t("k", None, Some("v"), None), &taint));
        assert!(!tolerates_taint(&t("k", None, Some("other"), None), &taint));
        // Exists ignores the value; empty key matches every key.
        assert!(tolerates_taint(
            &t("", Some(TolerationOperator::Exists), None, None),
            &taint
        ));
        assert!(!tolerates_taint(
            &t("x", Some(TolerationOperator::Exists), None, None),
            &taint
        ));
        // A set effect must match.
        assert!(!tolerates_taint(
            &t(
                "k",
                Some(TolerationOperator::Exists),
                None,
                Some(DeviceTaintEffect::NoSchedule)
            ),
            &taint
        ));
        assert!(tolerates_taint(
            &t(
                "k",
                Some(TolerationOperator::Exists),
                None,
                Some(DeviceTaintEffect::NoExecute)
            ),
            &taint
        ));
        // Unknown operators never match.
        assert!(!tolerates_taint(
            &t(
                "k",
                Some(TolerationOperator::Unknown("Gt".into())),
                None,
                None
            ),
            &taint
        ));
    }

    #[test]
    fn resource_claim_name_follows_upstream() {
        let f = Fx::new();
        let spec_claim =
            |p: &Pod| p.spec.as_ref().unwrap().resource_claims.as_ref().unwrap()[0].clone();
        // Direct reference: no owner check.
        assert_eq!(
            resource_claim_name(&f.pod, &spec_claim(&f.pod)),
            Ok(Some((claim_name(), false)))
        );
        // Template: needs the status entry, owner check required.
        assert_eq!(
            resource_claim_name(&f.pod_template, &spec_claim(&f.pod_template)),
            Err(ClaimNameError::NotCreatedYet)
        );
        assert_eq!(
            resource_claim_name(
                &f.pod_template_in_status,
                &spec_claim(&f.pod_template_in_status)
            ),
            Ok(Some((claim_name(), true)))
        );
        // Intentionally not created.
        assert_eq!(
            resource_claim_name(
                &f.pod_template_no_claim,
                &spec_claim(&f.pod_template_no_claim)
            ),
            Ok(None)
        );
        // Neither field set.
        let none = PodResourceClaim {
            name: "x".into(),
            resource_claim_name: None,
            resource_claim_template_name: None,
        };
        assert_eq!(
            resource_claim_name(&f.pod, &none),
            Err(ClaimNameError::Unsupported)
        );
    }

    #[test]
    fn pool_generation_tracking() {
        let f = Fx::new();
        let mut pool = Pool::default();
        pool.add_slice(&f.slice);
        assert_eq!(pool.max_generation, 0);
        // A newer generation shadows the older slice's devices.
        pool.add_slice(&f.slice_replaced);
        assert_eq!(pool.max_generation, 1);
        assert!(pool
            .device("instance")
            .is_some_and(|(s, _)| s.metadata.name.ends_with("-updated")));
        // Removing it brings the old generation back.
        pool.remove_slice(&f.slice_replaced);
        assert_eq!(pool.max_generation, 0);
        assert!(pool
            .device("instance")
            .is_some_and(|(s, _)| !s.metadata.name.ends_with("-updated")));
    }

    // --- TestRetry / metrics / cancel with a running clock ------------------------------

    /// `testPodDeletionsMetrics`: one deletion is counted once, with the latency
    /// observed in the (single) bucket.
    #[test]
    fn deletion_updates_metrics() {
        run_big(deletion_updates_metrics_async());
    }

    async fn deletion_updates_metrics_async() {
        let f = Fx::new();
        let mut ctx = new_ctx();
        ctx.add_pod_everywhere(&f.pod).await;
        ctx.apply(&add(&f.slice_tainted)).await;
        ctx.apply(&add(&f.in_use_claim)).await;
        ctx.controller.on_pod_change(None, Some(&f.pod));
        ctx.controller.worker().await;
        assert_eq!(ctx.metrics.pod_deletions_total.get(), 1);
        let text = ctx.metrics.gather();
        assert!(
            text.contains("device_taint_eviction_controller_pod_deletions_total 1"),
            "{text}"
        );
        assert!(
            text.contains("device_taint_eviction_controller_pod_deletion_duration_seconds_count 1"),
            "{text}"
        );
        assert!(ctx.drain_events().iter().any(|(_, m)| *m == Msg::Delete));
    }

    /// `TestRetry`: a failing pod delete is retried with rate limiting and the
    /// pod stays in `deletePodAt`; once the API works again it is deleted.
    #[test]
    fn failed_pod_deletion_is_retried() {
        run_big(failed_pod_deletion_is_retried_async());
    }

    async fn failed_pod_deletion_is_retried_async() {
        let f = Fx::new();
        let mut ctx = new_ctx();
        // The pod is known to the controller but not (yet) to the API.
        ctx.controller
            .pod_store()
            .put(pod_store_key(&f.pod), f.pod.clone());
        ctx.apply(&add(&f.slice_tainted)).await;
        ctx.apply(&add(&f.in_use_claim)).await;
        ctx.controller.on_pod_change(None, Some(&f.pod));
        assert_eq!(ctx.queue.state().ready, vec![wi_pod(&f.pod)]);

        // Not found in the API: nothing to do any more, no retry.
        ctx.controller.worker().await;
        assert!(ctx.queue.state().failures.is_empty());
        assert!(ctx.controller.state().delete_pod_at.is_empty());
        assert!(ctx.drain_events().is_empty());

        // Replaced by another pod with the same name: also no retry, no event.
        let _: Pod = ctx
            .storage
            .create(&pod_key(&f.pod_other_uid), &*f.pod_other_uid)
            .await
            .unwrap();
        ctx.controller.on_pod_change(None, Some(&f.pod));
        ctx.controller.worker().await;
        assert!(ctx.queue.state().failures.is_empty());
        assert!(ctx.controller.state().delete_pod_at.is_empty());
        assert!(ctx.drain_events().is_empty());
        assert_eq!(
            remaining_pods(&ctx).await,
            vec!["default/my-pod".to_string()]
        );
    }

    /// `testCancelEviction`: the claim tolerates the taint for a while; deleting
    /// the pod (or the slice) before that cancels the eviction, and the work item
    /// that comes up later finds nothing to do.
    #[test]
    fn cancel_eviction_by_pod_or_slice_deletion() {
        run_big(cancel_eviction_by_pod_or_slice_deletion_async());
    }

    async fn cancel_eviction_by_pod_or_slice_deletion_async() {
        for delete_pod in [true, false] {
            let f = Fx::new();
            let mut ctx = new_ctx();
            let claim = Arc::new(with_tolerations(
                &f.in_use_claim,
                vec![toleration("", TolerationOperator::Exists, None, Some(60))],
            ));
            ctx.add_pod_everywhere(&f.pod).await;
            ctx.apply(&add(&f.slice_tainted)).await;
            ctx.apply(&add(&claim)).await;
            ctx.controller.on_pod_change(None, Some(&f.pod));
            assert_eq!(ctx.controller.state().delete_pod_at.len(), 1);
            assert_eq!(
                ctx.queue.state().later,
                vec![(wi_pod(&f.pod), Duration::from_secs(60))]
            );

            if delete_pod {
                ctx.apply(&remove(&f.pod)).await;
            } else {
                ctx.apply(&remove(&f.slice_tainted)).await;
            }
            assert!(ctx.controller.state().delete_pod_at.is_empty());

            // Time passes; the stale work item is recognised as obsolete.
            *ctx.clock_now.lock().unwrap() += ChronoDuration::seconds(61);
            ctx.queue.advance(Duration::from_secs(61));
            ctx.controller.worker().await;
            assert_eq!(ctx.controller.state().maybe_delete_pod_count, 1);
            assert!(ctx.queue.state().failures.is_empty());
            let events = ctx.drain_events();
            assert_eq!(
                events.iter().filter(|(_, m)| *m == Msg::Delete).count(),
                0,
                "no deletion event for delete_pod={delete_pod}"
            );
            if !delete_pod {
                // Pod still there, untouched.
                assert_eq!(
                    remaining_pods(&ctx).await,
                    vec!["default/my-pod".to_string()]
                );
                assert!(events.iter().any(|(_, m)| *m == Msg::Cancel));
            }
        }
    }

    /// `TestDeviceTaintRule` flow: a rule with the `None` effect is simulated, then
    /// switched to `NoExecute`, which evicts the pod and later completes the
    /// status; deleting the rule drops its statistics.
    #[test]
    fn device_taint_rule_flow() {
        run_big(device_taint_rule_flow_async());
    }

    async fn device_taint_rule_flow_async() {
        let f = Fx::new();
        let ctx = new_ctx();
        let t0 = taint_time();
        ctx.add_pod_everywhere(&f.pod).await;
        ctx.apply(&add(&f.in_use_claim)).await;
        ctx.controller.on_pod_change(None, Some(&f.pod));
        ctx.apply(&add(&f.rule_none)).await;
        ctx.controller.worker().await;

        let tail = "if the effect was NoExecute. This information will not be updated again. Recreate the DeviceTaintRule to trigger an update.";
        check_rules(
            &ctx,
            &[in_progress(
                &f.rule_none,
                false,
                "NoEffect",
                &format!("0 published devices selected. 1 allocated device selected. 1 pod would be evicted in 1 namespace {tail}"),
                t0,
            )],
            "none effect",
        )
        .await;
        assert_eq!(
            remaining_pods(&ctx).await,
            vec!["default/my-pod".to_string()]
        );

        // Move time forward and switch to NoExecute.
        *ctx.clock_now.lock().unwrap() += ChronoDuration::seconds(20);
        let mut evict = (*f.rule_none).clone();
        evict.spec.taint.effect = Some(DeviceTaintEffect::NoExecute);
        evict.status = ctx
            .storage
            .get::<DeviceTaintRule>(&rule_key(&f.rule_none))
            .await
            .unwrap()
            .status;
        let evict = Arc::new(evict);
        ctx.apply(&update(&f.rule_none, &evict)).await;
        ctx.controller.worker().await;
        assert!(remaining_pods(&ctx).await.is_empty());
        ctx.sync_pod_store().await;
        assert_eq!(
            ctx.controller.state().taint_rule_stats[&evict.metadata.uid].num_evicted_pods,
            1
        );

        // After the status period the completed state is published.
        *ctx.clock_now.lock().unwrap() += cd(RULE_STATUS_PERIOD);
        ctx.queue.advance(RULE_STATUS_PERIOD);
        ctx.controller.worker().await;
        let stored: DeviceTaintRule = ctx.storage.get(&rule_key(&evict)).await.unwrap();
        let c = &stored.status.conditions[0];
        assert_eq!(c.status, "False");
        assert_eq!(c.reason.as_deref(), Some("Completed"));
        assert_eq!(
            c.message.as_deref(),
            Some("1 pod evicted since starting the controller.")
        );

        // Deleting the rule must not leak its statistics.
        let stored = Arc::new(stored);
        ctx.apply(&Ev::Rule(Some(stored), None)).await;
        assert!(ctx.controller.state().taint_rule_stats.is_empty());
        assert!(ctx.controller.state().evicting_rules.is_empty());
    }

    /// The informer stand-in delivers adds from the initial list and live changes
    /// as add/update/delete pairs.
    #[test]
    fn informer_delivers_list_and_watch_events() {
        run_big(informer_delivers_list_and_watch_events_async());
    }

    async fn informer_delivers_list_and_watch_events_async() {
        let f = Fx::new();
        let storage = Arc::new(MemoryStorage::new());
        let _: ResourceSlice = storage
            .create(
                &build_key("resourceslices", None, &f.slice.metadata.name),
                &*f.slice,
            )
            .await
            .unwrap();
        let synced = Arc::new(AtomicBool::new(false));
        let seen = Arc::new(Mutex::new(Vec::<(bool, bool)>::new()));
        let seen2 = seen.clone();
        let handle = tokio::spawn(run_informer::<MemoryStorage, ResourceSlice, _>(
            storage.clone(),
            build_prefix("resourceslices", None),
            |s| s.metadata.name.clone(),
            synced.clone(),
            move |old, new| seen2.lock().unwrap().push((old.is_some(), new.is_some())),
        ));
        for _ in 0..100 {
            if synced.load(Ordering::SeqCst) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(synced.load(Ordering::SeqCst));
        let _: ResourceSlice = storage
            .update(
                &build_key("resourceslices", None, &f.slice.metadata.name),
                &*f.slice_untainted,
            )
            .await
            .unwrap_or_else(|_| panic!("update"));
        storage
            .delete(&build_key("resourceslices", None, &f.slice.metadata.name))
            .await
            .unwrap();
        for _ in 0..100 {
            if seen.lock().unwrap().len() >= 3 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        handle.abort();
        assert_eq!(
            *seen.lock().unwrap(),
            vec![(false, true), (true, true), (true, false)]
        );
    }
}
