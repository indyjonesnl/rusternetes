//! Port of `selinux_warning_controller_test.go` `TestSELinuxWarningController_Sync`
//! (release-1.35) plus the `GetMountSELinuxLabel` / `GetPodVolumeNames` edge
//! cases the controller depends on.

use super::*;
use rusternetes_common::feature_gates::with_feature;
use rusternetes_common::resources::Event;
use rusternetes_storage::memory::MemoryStorage;
use serde_json::{json, Value};
use serial_test::serial;
use std::collections::HashMap;

const NAMESPACE: &str = "ns1";
const PVC_UID: &str = "uid1";
const MOUNT: &str = SELINUX_CHANGE_POLICY_MOUNT_OPTION;
const RECURSIVE: &str = SELINUX_CHANGE_POLICY_RECURSIVE;

// ---- fixtures (pv / pvc / pod builders of the upstream test) --------------

fn pv(name: &str) -> Value {
    json!({
        "apiVersion": "v1", "kind": "PersistentVolume",
        "metadata": {"name": name},
        "spec": {"awsElasticBlockStore": {"volumeID": name}},
    })
}

fn pvc(name: &str) -> Value {
    json!({
        "apiVersion": "v1", "kind": "PersistentVolumeClaim",
        "metadata": {"name": name, "namespace": NAMESPACE, "uid": PVC_UID},
        "spec": {"accessModes": ["ReadWriteOnce"]},
    })
}

fn pv_bound_to_pvc(pv_name: &str, pvc_name: &str) -> Value {
    let mut v = pv(pv_name);
    v["spec"]["claimRef"] = json!({
        "kind": "PersistentVolumeClaim", "namespace": NAMESPACE,
        "name": pvc_name, "uid": PVC_UID, "apiVersion": "v1",
    });
    v["status"] = json!({"phase": "Bound"});
    v
}

fn pvc_bound_to_pv(pv_name: &str, pvc_name: &str) -> Value {
    let mut v = pvc(pvc_name);
    v["spec"]["volumeName"] = json!(pv_name);
    v["status"] = json!({"phase": "Bound"});
    v
}

struct PodBuilder(Value);

fn pod(name: &str, level: &str, change_policy: Option<&str>) -> PodBuilder {
    let mut sc = json!({});
    if let Some(p) = change_policy {
        sc["seLinuxChangePolicy"] = json!(p);
    }
    if !level.is_empty() {
        sc["seLinuxOptions"] = json!({"level": level});
    }
    PodBuilder(json!({
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"namespace": NAMESPACE, "name": name},
        "spec": {
            "containers": [{
                "name": "container1", "image": "image1",
                "volumeMounts": [{"name": "vol1", "mountPath": "/mnt"}],
            }],
            "securityContext": sc,
            "volumes": [{"name": "emptyDir1", "emptyDir": {}}],
        },
        "status": {"phase": "Running"},
    }))
}

impl PodBuilder {
    fn with_phase(mut self, phase: &str) -> Self {
        self.0["status"]["phase"] = json!(phase);
        self
    }
    fn with_inline_volume(mut self) -> Self {
        self.0["spec"]["volumes"]
            .as_array_mut()
            .unwrap()
            .push(json!({"name": "inlineVolume",
                "awsElasticBlockStore": {"volumeID": "inlinevol1"}}));
        self.0["spec"]["containers"][0]["volumeMounts"]
            .as_array_mut()
            .unwrap()
            .push(json!({"name": "inlineVolume", "mountPath": "/mnt"}));
        self
    }
    fn with_pvc(mut self, pvc_name: &str, volume_name: &str) -> Self {
        self.0["spec"]["volumes"]
            .as_array_mut()
            .unwrap()
            .push(json!({"name": volume_name, "persistentVolumeClaim": {"claimName": pvc_name}}));
        self.0["spec"]["containers"][0]["volumeMounts"]
            .as_array_mut()
            .unwrap()
            .push(json!({"name": volume_name, "mountPath": "/mnt"}));
        self
    }
    fn build(self) -> Pod {
        serde_json::from_value(self.0).unwrap()
    }
}

// ---- fake volume plugin + fake volume cache --------------------------------

/// `volumetesting.FakeVolumePlugin` (`GetTestKubeletVolumePluginMgr`), reduced
/// to what the controller calls: it supports every spec and names it after
/// `Spec.Name()`.
struct FakePlugin {
    supports_selinux: bool,
}

#[async_trait]
impl VolumePlugin for FakePlugin {
    fn plugin_name(&self) -> &str {
        "fake-plugin"
    }
    fn can_support(&self, _: &VolumeSpec) -> bool {
        true
    }
    fn get_volume_name(&self, spec: &VolumeSpec) -> Result<String, String> {
        Ok(spec_name(spec))
    }
    async fn supports_selinux_context_mount(&self, _: &VolumeSpec) -> Result<bool, String> {
        Ok(self.supports_selinux)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct AddedVolume {
    volume_name: String,
    pod_key: ObjectName,
    label: String,
    change_policy: String,
    csi_driver: String,
}

fn added(volume: &str, pod: &str, label: &str, policy: &str, driver: &str) -> AddedVolume {
    AddedVolume {
        volume_name: volume.to_string(),
        pod_key: ObjectName::new(NAMESPACE, pod),
        label: label.to_string(),
        change_policy: policy.to_string(),
        csi_driver: driver.to_string(),
    }
}

#[derive(Default)]
struct FakeVolumeCache {
    added_volumes: Mutex<Vec<AddedVolume>>,
    /// Conflicts to send when AddVolume is called for the given pod.
    conflicts_to_send: HashMap<ObjectName, Vec<Conflict>>,
    deleted_pods: Mutex<Vec<ObjectName>>,
}

impl VolumeCache for FakeVolumeCache {
    fn add_volume(
        &self,
        volume_name: &str,
        pod_key: &ObjectName,
        label: &str,
        change_policy: &str,
        csi_driver: &str,
    ) -> Vec<Conflict> {
        self.added_volumes.lock().unwrap().push(AddedVolume {
            volume_name: volume_name.to_string(),
            pod_key: pod_key.clone(),
            label: label.to_string(),
            change_policy: change_policy.to_string(),
            csi_driver: csi_driver.to_string(),
        });
        self.conflicts_to_send
            .get(pod_key)
            .cloned()
            .unwrap_or_default()
    }
    fn delete_pod(&self, pod_key: &ObjectName) {
        self.deleted_pods.lock().unwrap().push(pod_key.clone());
    }
    fn get_pods_for_csi_driver(&self, driver_name: &str) -> Vec<ObjectName> {
        self.added_volumes
            .lock()
            .unwrap()
            .iter()
            .filter(|v| v.csi_driver == driver_name)
            .map(|v| v.pod_key.clone())
            .collect()
    }
    fn get_conflicts(&self) -> Vec<Conflict> {
        self.conflicts_to_send.values().flatten().cloned().collect()
    }
}

fn label_conflict(pod: &str, value: &str, other: &str, other_value: &str) -> Conflict {
    Conflict {
        property_name: "SELinuxLabel".into(),
        event_reason: "SELinuxLabelConflict".into(),
        pod: ObjectName::new(NAMESPACE, pod),
        property_value: value.into(),
        other_pod: ObjectName::new(NAMESPACE, other),
        other_property_value: other_value.into(),
    }
}

fn policy_conflict(pod: &str, value: &str, other: &str, other_value: &str) -> Conflict {
    Conflict {
        property_name: "SELinuxChangePolicy".into(),
        event_reason: "SELinuxChangePolicyConflict".into(),
        pod: ObjectName::new(NAMESPACE, pod),
        property_value: value.into(),
        other_pod: ObjectName::new(NAMESPACE, other),
        other_property_value: other_value.into(),
    }
}

// ---- TestSELinuxWarningController_Sync --------------------------------------

struct Case {
    name: &'static str,
    existing_pvcs: Vec<Value>,
    existing_pvs: Vec<Value>,
    existing_pods: Vec<Pod>,
    pod: &'static str,
    csi_driver_selinux_enabled: bool,
    conflicts: Vec<Conflict>,
    expect_error: bool,
    expected_added_volumes: Vec<AddedVolume>,
    expected_events: Vec<String>,
    expected_deleted_pods: Vec<ObjectName>,
}

impl Default for Case {
    fn default() -> Self {
        Self {
            name: "",
            existing_pvcs: vec![],
            existing_pvs: vec![],
            existing_pods: vec![],
            pod: "pod1",
            csi_driver_selinux_enabled: true,
            conflicts: vec![],
            expect_error: false,
            expected_added_volumes: vec![],
            expected_events: vec![],
            expected_deleted_pods: vec![],
        }
    }
}

fn bound_pv_pvc() -> (Vec<Value>, Vec<Value>) {
    (
        vec![pvc_bound_to_pv("pv1", "pvc1")],
        vec![pv_bound_to_pvc("pv1", "pvc1")],
    )
}

fn cases() -> Vec<Case> {
    const EBS: &str = "ebs.csi.aws.com";
    let (pvcs, pvs) = bound_pv_pvc();
    let ev_conflict = |reason: &str, prop: &str, v: &str, other: &str, ov: &str| {
        format!(
            "Normal {reason} {prop} \"{v}\" conflicts with pod {other} that uses the same volume as this pod with {prop} \"{ov}\". If both pods land on the same node, only one of them may access the volume."
        )
    };
    vec![
        Case {
            name: "existing pod with no volumes",
            existing_pods: vec![pod("pod1", "s0:c1,c2", None).build()],
            ..Default::default()
        },
        Case {
            name: "existing pod with unbound PVC",
            existing_pods: vec![pod("pod1", "s0:c1,c2", None)
                .with_pvc("non-existing-pvc", "vol1")
                .build()],
            // PVC is missing, add back to queue with exp. backoff
            expect_error: true,
            ..Default::default()
        },
        Case {
            name: "existing pod with fully bound PVC",
            existing_pvcs: pvcs.clone(),
            existing_pvs: pvs.clone(),
            existing_pods: vec![pod("pod1", "s0:c1,c2", None).with_pvc("pvc1", "vol1").build()],
            expected_added_volumes: vec![added(
                "fake-plugin/pv1",
                "pod1",
                ":::s0:c1,c2",
                MOUNT,
                EBS,
            )],
            ..Default::default()
        },
        Case {
            name: "existing pod with fully bound PVC, Recursive change policy",
            existing_pvcs: pvcs.clone(),
            existing_pvs: pvs.clone(),
            existing_pods: vec![pod("pod1", "s0:c1,c2", Some(RECURSIVE))
                .with_pvc("pvc1", "vol1")
                .build()],
            // Label is cleared with the Recursive policy.
            expected_added_volumes: vec![added("fake-plugin/pv1", "pod1", "", RECURSIVE, EBS)],
            ..Default::default()
        },
        Case {
            name: "existing pod with inline volume",
            existing_pvcs: pvcs.clone(),
            existing_pvs: pvs.clone(),
            existing_pods: vec![pod("pod1", "s0:c1,c2", None).with_inline_volume().build()],
            expected_added_volumes: vec![added(
                "fake-plugin/ebs.csi.aws.com-inlinevol1",
                "pod1",
                ":::s0:c1,c2",
                MOUNT,
                EBS,
            )],
            ..Default::default()
        },
        Case {
            name: "existing pod with inline volume and PVC",
            existing_pvcs: pvcs.clone(),
            existing_pvs: pvs.clone(),
            existing_pods: vec![pod("pod1", "s0:c1,c2", None)
                .with_pvc("pvc1", "vol1")
                .with_inline_volume()
                .build()],
            expected_added_volumes: vec![
                added("fake-plugin/pv1", "pod1", ":::s0:c1,c2", MOUNT, EBS),
                added(
                    "fake-plugin/ebs.csi.aws.com-inlinevol1",
                    "pod1",
                    ":::s0:c1,c2",
                    MOUNT,
                    EBS,
                ),
            ],
            ..Default::default()
        },
        Case {
            name: "existing pod with PVC generates conflict, the other pod exists",
            existing_pvcs: pvcs.clone(),
            existing_pvs: pvs.clone(),
            existing_pods: vec![
                pod("pod1", "s0:c1,c2", None).with_pvc("pvc1", "vol1").build(),
                pod("pod2", "s0:c98,c99", None).build(),
            ],
            conflicts: vec![
                label_conflict("pod1", ":::s0:c1,c2", "pod2", ":::s0:c98,c99"),
                label_conflict("pod2", ":::s0:c98,c99", "pod1", ":::s0:c1,c2"),
            ],
            expected_added_volumes: vec![added(
                "fake-plugin/pv1",
                "pod1",
                ":::s0:c1,c2",
                MOUNT,
                EBS,
            )],
            expected_events: vec![
                ev_conflict("SELinuxLabelConflict", "SELinuxLabel", ":::s0:c1,c2", "pod2", ":::s0:c98,c99"),
                ev_conflict("SELinuxLabelConflict", "SELinuxLabel", ":::s0:c98,c99", "pod1", ":::s0:c1,c2"),
            ],
            ..Default::default()
        },
        Case {
            name: "existing pod with Recursive policy does not generate conflicts",
            existing_pvcs: pvcs.clone(),
            existing_pvs: pvs.clone(),
            existing_pods: vec![
                pod("pod1", "s0:c1,c2", Some(RECURSIVE))
                    .with_pvc("pvc1", "vol1")
                    .build(),
                pod("pod2", "s0:c98,c99", Some(RECURSIVE)).build(),
            ],
            expected_added_volumes: vec![added("fake-plugin/pv1", "pod1", "", RECURSIVE, EBS)],
            ..Default::default()
        },
        Case {
            name: "existing pod with Recursive policy does not conflict with pod with MountOption policy label, only with the policy",
            existing_pvcs: pvcs.clone(),
            existing_pvs: pvs.clone(),
            existing_pods: vec![
                pod("pod1", "s0:c1,c2", Some(RECURSIVE))
                    .with_pvc("pvc1", "vol1")
                    .build(),
                pod("pod2", "s0:c98,c99", Some(MOUNT))
                    .with_pvc("pvc1", "vol1")
                    .build(),
            ],
            conflicts: vec![
                policy_conflict("pod1", RECURSIVE, "pod2", MOUNT),
                policy_conflict("pod2", MOUNT, "pod1", RECURSIVE),
            ],
            expected_added_volumes: vec![added("fake-plugin/pv1", "pod1", "", RECURSIVE, EBS)],
            expected_events: vec![
                ev_conflict("SELinuxChangePolicyConflict", "SELinuxChangePolicy", RECURSIVE, "pod2", MOUNT),
                ev_conflict("SELinuxChangePolicyConflict", "SELinuxChangePolicy", MOUNT, "pod1", RECURSIVE),
            ],
            ..Default::default()
        },
        Case {
            name: "existing pod with PVC generates conflict, the other pod doesn't exist",
            existing_pvcs: pvcs.clone(),
            existing_pvs: pvs.clone(),
            // "pod2" does not exist
            existing_pods: vec![pod("pod1", "s0:c1,c2", None).with_pvc("pvc1", "vol1").build()],
            conflicts: vec![
                label_conflict("pod1", ":::s0:c1,c2", "pod2", ":::s0:c98,c99"),
                label_conflict("pod2", ":::s0:c98,c99", "pod1", ":::s0:c1,c2"),
            ],
            expected_added_volumes: vec![added(
                "fake-plugin/pv1",
                "pod1",
                ":::s0:c1,c2",
                MOUNT,
                EBS,
            )],
            // Event for the missing pod is not sent.
            expected_events: vec![ev_conflict(
                "SELinuxLabelConflict",
                "SELinuxLabel",
                ":::s0:c1,c2",
                "pod2",
                ":::s0:c98,c99",
            )],
            ..Default::default()
        },
        Case {
            name: "empty label implies Recursive policy",
            existing_pvcs: pvcs.clone(),
            existing_pvs: pvs.clone(),
            existing_pods: vec![pod("pod1", "", Some(MOUNT)).with_pvc("pvc1", "vol1").build()],
            // Reset to Recursive when the label is empty.
            expected_added_volumes: vec![added("fake-plugin/pv1", "pod1", "", RECURSIVE, EBS)],
            ..Default::default()
        },
        Case {
            name: "pending pod is processed",
            existing_pvcs: pvcs.clone(),
            existing_pvs: pvs.clone(),
            existing_pods: vec![pod("pod1", "s0:c1,c2", None)
                .with_pvc("pvc1", "vol1")
                .with_phase("Pending")
                .build()],
            expected_added_volumes: vec![added(
                "fake-plugin/pv1",
                "pod1",
                ":::s0:c1,c2",
                MOUNT,
                EBS,
            )],
            ..Default::default()
        },
        Case {
            name: "unknown pod is processed",
            existing_pvcs: pvcs.clone(),
            existing_pvs: pvs.clone(),
            existing_pods: vec![pod("pod1", "s0:c1,c2", None)
                .with_pvc("pvc1", "vol1")
                .with_phase("Unknown")
                .build()],
            expected_added_volumes: vec![added(
                "fake-plugin/pv1",
                "pod1",
                ":::s0:c1,c2",
                MOUNT,
                EBS,
            )],
            ..Default::default()
        },
        Case {
            name: "succeeded pod is removed from the cache",
            existing_pvcs: pvcs.clone(),
            existing_pvs: pvs.clone(),
            existing_pods: vec![pod("pod1", "s0:c1,c2", None)
                .with_pvc("pvc1", "vol1")
                .with_phase("Succeeded")
                .build()],
            expected_deleted_pods: vec![ObjectName::new(NAMESPACE, "pod1")],
            ..Default::default()
        },
        Case {
            name: "failed pod is removed from the cache",
            existing_pvcs: pvcs.clone(),
            existing_pvs: pvs.clone(),
            existing_pods: vec![pod("pod1", "s0:c1,c2", None)
                .with_pvc("pvc1", "vol1")
                .with_phase("Failed")
                .build()],
            expected_deleted_pods: vec![ObjectName::new(NAMESPACE, "pod1")],
            ..Default::default()
        },
        Case {
            name: "deleted pod",
            // "pod1" does not exist in storage
            expected_deleted_pods: vec![ObjectName::new(NAMESPACE, "pod1")],
            ..Default::default()
        },
        Case {
            name: "existing pod with fully bound PVC and CSIDriver.SELinuxMount disabled",
            existing_pvcs: pvcs,
            existing_pvs: pvs,
            existing_pods: vec![pod("pod1", "s0:c1,c2", None).with_pvc("pvc1", "vol1").build()],
            csi_driver_selinux_enabled: false,
            // Label is cleared and the policy reset to Recursive when the CSI
            // driver does not support SELinuxMount.
            expected_added_volumes: vec![added("fake-plugin/pv1", "pod1", "", RECURSIVE, EBS)],
            ..Default::default()
        },
    ]
}

async fn collect_events(storage: &MemoryStorage) -> Vec<String> {
    let events: Vec<Event> = storage
        .list(&build_prefix("events", None))
        .await
        .expect("list events");
    events
        .into_iter()
        .map(|e| format!("{:?} {} {}", e.event_type, e.reason, e.message))
        .collect()
}

#[tokio::test]
#[serial]
async fn sync() {
    for tt in cases() {
        let _g1 = with_feature(Feature::SELinuxChangePolicy, true);
        let _g2 = with_feature(Feature::SELinuxMountReadWriteOncePod, true);

        let storage = Arc::new(MemoryStorage::new());
        let cache = Arc::new(FakeVolumeCache {
            conflicts_to_send: HashMap::from([(ObjectName::new(NAMESPACE, tt.pod), tt.conflicts)]),
            ..Default::default()
        });
        let c = SELinuxWarningController::new(storage.clone())
            .with_plugins(vec![Arc::new(FakePlugin {
                supports_selinux: tt.csi_driver_selinux_enabled,
            })])
            .with_label_cache(cache.clone());

        // Inject fake existing objects.
        for v in &tt.existing_pvcs {
            let p: PersistentVolumeClaim = serde_json::from_value(v.clone()).unwrap();
            let key = build_key("persistentvolumeclaims", Some(NAMESPACE), &p.metadata.name);
            storage.create(&key, &p).await.unwrap();
        }
        for v in &tt.existing_pvs {
            let p: PersistentVolume = serde_json::from_value(v.clone()).unwrap();
            let key = build_key("persistentvolumes", None, &p.metadata.name);
            storage.create(&key, &p).await.unwrap();
        }
        for p in &tt.existing_pods {
            let key = build_key("pods", Some(NAMESPACE), &p.metadata.name);
            storage.create(&key, p).await.unwrap();
        }

        // Act: sync() the pod.
        let result = c.sync(&ObjectName::new(NAMESPACE, tt.pod)).await;

        if tt.expect_error {
            assert!(result.is_err(), "{}: expected error, got Ok", tt.name);
            continue; // do not check the rest on error
        }
        result.unwrap_or_else(|e| panic!("{}: unexpected error: {e}", tt.name));

        let mut got = cache.added_volumes.lock().unwrap().clone();
        let mut want = tt.expected_added_volumes.clone();
        got.sort();
        want.sort();
        assert_eq!(got, want, "{}: added volumes", tt.name);

        let received: std::collections::BTreeSet<String> =
            collect_events(&storage).await.into_iter().collect();
        let expected: std::collections::BTreeSet<String> =
            tt.expected_events.iter().cloned().collect();
        assert_eq!(received, expected, "{}: events", tt.name);

        assert_eq!(
            *cache.deleted_pods.lock().unwrap(),
            tt.expected_deleted_pods,
            "{}: deleted pods",
            tt.name
        );
    }
}

// ---- end-to-end with the real cache -----------------------------------------

/// Two pods on one PV with different levels: the real cache reports the
/// conflict once on the second pod (both pods get the event) and the metric
/// exposes both directions; deleting a pod clears it.
#[tokio::test]
#[serial]
async fn real_cache_reports_label_conflict_between_pods_sharing_a_volume() {
    let _g1 = with_feature(Feature::SELinuxChangePolicy, true);
    let _g2 = with_feature(Feature::SELinuxMountReadWriteOncePod, true);
    let storage = Arc::new(MemoryStorage::new());
    let cache: Arc<dyn VolumeCache> = Arc::new(VolumeLabelCache::new(ControllerSELinuxTranslator));
    let c = SELinuxWarningController::new(storage.clone())
        .with_plugins(vec![Arc::new(FakePlugin {
            supports_selinux: true,
        })])
        .with_label_cache(cache.clone());

    let (pvcs, pvs) = bound_pv_pvc();
    for v in pvcs {
        let p: PersistentVolumeClaim = serde_json::from_value(v).unwrap();
        let key = build_key("persistentvolumeclaims", Some(NAMESPACE), &p.metadata.name);
        storage.create(&key, &p).await.unwrap();
    }
    for v in pvs {
        let p: PersistentVolume = serde_json::from_value(v).unwrap();
        storage
            .create(&build_key("persistentvolumes", None, &p.metadata.name), &p)
            .await
            .unwrap();
    }
    for p in [
        pod("pod1", "s0:c1,c2", None)
            .with_pvc("pvc1", "vol1")
            .build(),
        pod("pod2", "s0:c98,c99", None)
            .with_pvc("pvc1", "vol1")
            .build(),
    ] {
        storage
            .create(&build_key("pods", Some(NAMESPACE), &p.metadata.name), &p)
            .await
            .unwrap();
    }

    c.sync(&ObjectName::new(NAMESPACE, "pod1")).await.unwrap();
    assert!(collect_events(&storage).await.is_empty());
    c.sync(&ObjectName::new(NAMESPACE, "pod2")).await.unwrap();
    let events = collect_events(&storage).await;
    assert_eq!(events.len(), 2, "{events:?}");
    assert!(events
        .iter()
        .all(|e| e.starts_with("Normal SELinuxLabelConflict ")));

    let out = metrics::render(cache.as_ref());
    assert!(out.contains("pod1_name=\"pod1\""), "{out}");
    assert!(out.contains("pod1_name=\"pod2\""), "{out}");

    // Re-syncing an unchanged pod emits nothing new.
    c.sync(&ObjectName::new(NAMESPACE, "pod2")).await.unwrap();
    assert_eq!(collect_events(&storage).await.len(), 2);

    // The pod finishing clears its conflicts from the cache.
    let mut done = pod("pod2", "s0:c98,c99", None)
        .with_pvc("pvc1", "vol1")
        .build();
    done.status.as_mut().unwrap().phase = Some(Phase::Succeeded);
    storage
        .update(&build_key("pods", Some(NAMESPACE), "pod2"), &done)
        .await
        .unwrap();
    c.sync(&ObjectName::new(NAMESPACE, "pod2")).await.unwrap();
    assert_eq!(metrics::render(cache.as_ref()), "");
}

// ---- GetMountSELinuxLabel / GetPodVolumeNames --------------------------------

fn opts(level: &str) -> SELinuxOptions {
    SELinuxOptions {
        user: None,
        role: None,
        type_: None,
        level: Some(level.to_string()),
    }
}

#[tokio::test]
#[serial]
async fn multiple_labels_for_one_volume_is_an_error_with_event() {
    let _g = with_feature(Feature::SELinuxMountReadWriteOncePod, true);
    let spec = VolumeSpec {
        volume: Some(
            serde_json::from_value(json!({"name": "v", "hostPath": {"path": "/x"}})).unwrap(),
        ),
        ..Default::default()
    };
    let plugins: Vec<Arc<dyn VolumePlugin>> = vec![Arc::new(FakePlugin {
        supports_selinux: true,
    })];
    let err = get_mount_selinux_label(
        &spec,
        &[opts("s0:c1"), opts("s0:c2")],
        None,
        &plugins,
        &ControllerSELinuxTranslator,
    )
    .await
    .unwrap_err();
    assert_eq!(
        err,
        SELinuxLabelError::MultipleLabels(vec![":::s0:c1".into(), ":::s0:c2".into()])
    );
    assert_eq!(
        err.to_string(),
        "multiple SELinux labels found: :::s0:c1,:::s0:c2"
    );

    // The same label twice is squashed by the set.
    let ok = get_mount_selinux_label(
        &spec,
        &[opts("s0:c1"), opts("s0:c1")],
        None,
        &plugins,
        &ControllerSELinuxTranslator,
    )
    .await
    .unwrap();
    assert_eq!(ok.selinux_mount_label, ":::s0:c1");
    assert!(ok.plugin_supports_selinux_context_mount);

    // End to end: the pod gets the MultipleSELinuxLabels warning event.
    let _g1 = with_feature(Feature::SELinuxChangePolicy, true);
    let storage = Arc::new(MemoryStorage::new());
    let c = SELinuxWarningController::new(storage.clone())
        .with_plugins(plugins.clone())
        .with_label_cache(Arc::new(FakeVolumeCache::default()));
    let mut p = pod("pod1", "", None).build();
    {
        let spec = p.spec.as_mut().unwrap();
        spec.volumes = Some(vec![serde_json::from_value(
            json!({"name": "vol1", "hostPath": {"path": "/x"}}),
        )
        .unwrap()]);
        let mut second = spec.containers[0].clone();
        second.name = "container2".into();
        spec.containers[0].security_context =
            Some(serde_json::from_value(json!({"seLinuxOptions": {"level": "s0:c1"}})).unwrap());
        second.security_context =
            Some(serde_json::from_value(json!({"seLinuxOptions": {"level": "s0:c2"}})).unwrap());
        spec.containers.push(second);
    }
    storage
        .create(&build_key("pods", Some(NAMESPACE), "pod1"), &p)
        .await
        .unwrap();
    let res = c.sync(&ObjectName::new(NAMESPACE, "pod1")).await;
    assert!(res.is_err(), "the error requeues the pod");
    assert_eq!(
        collect_events(&storage).await,
        vec![
            "Warning MultipleSELinuxLabels Volume \"vol1\" is mounted twice with different SELinux labels inside this pod"
                .to_string()
        ]
    );
}

#[test]
fn pod_volume_names_use_container_options_over_pod_options() {
    let mut p = pod("pod1", "s0:pod", None).build();
    {
        let spec = p.spec.as_mut().unwrap();
        let mut init = spec.containers[0].clone();
        init.name = "init".into();
        init.volume_mounts = Some(vec![serde_json::from_value(
            json!({"name": "vol2", "mountPath": "/b"}),
        )
        .unwrap()]);
        init.security_context =
            Some(serde_json::from_value(json!({"seLinuxOptions": {"level": "s0:init"}})).unwrap());
        spec.init_containers = Some(vec![init]);
    }
    let (mounts, labels) = get_pod_volume_names(&p);
    assert_eq!(
        mounts.into_iter().collect::<Vec<_>>(),
        vec!["vol1".to_string(), "vol2".to_string()]
    );
    assert_eq!(labels["vol1"][0].level.as_deref(), Some("s0:pod"));
    assert_eq!(labels["vol2"][0].level.as_deref(), Some("s0:init"));

    // No options anywhere: nothing is recorded for the mount.
    let (_, labels) = get_pod_volume_names(&pod("pod1", "", None).build());
    assert!(labels.is_empty());
}

#[tokio::test]
#[serial]
async fn csi_plugin_reads_selinux_mount_from_the_csi_driver() {
    let _g = with_feature(Feature::SELinuxMountReadWriteOncePod, true);
    let storage = Arc::new(MemoryStorage::new());
    let plugin = CsiPlugin {
        storage: storage.clone(),
    };
    let spec = VolumeSpec {
        persistent_volume: Some(
            serde_json::from_value(json!({
                "metadata": {"name": "pv"},
                "spec": {"csi": {"driver": "d.example.com", "volumeHandle": "h1"}},
            }))
            .unwrap(),
        ),
        ..Default::default()
    };
    assert_eq!(plugin.get_volume_name(&spec).unwrap(), "d.example.com^h1");
    // No CSIDriver object: SELinuxMount defaults to false.
    assert_eq!(
        plugin.supports_selinux_context_mount(&spec).await,
        Ok(false)
    );
    for (mount, want) in [(None, false), (Some(false), false), (Some(true), true)] {
        let mut d: CSIDriver = serde_json::from_value(json!({
            "apiVersion": "storage.k8s.io/v1", "kind": "CSIDriver",
            "metadata": {"name": "d.example.com"}, "spec": {},
        }))
        .unwrap();
        d.spec.se_linux_mount = mount;
        let key = build_key("csidrivers", None, "d.example.com");
        let _ = storage.delete(&key).await;
        storage.create(&key, &d).await.unwrap();
        assert_eq!(
            plugin.supports_selinux_context_mount(&spec).await,
            Ok(want),
            "seLinuxMount={mount:?}"
        );
    }
    // With the gate off the plugin never claims support.
    let _off = with_feature(Feature::SELinuxMountReadWriteOncePod, false);
    assert_eq!(
        plugin.supports_selinux_context_mount(&spec).await,
        Ok(false)
    );
}

#[test]
fn find_plugin_by_spec_requires_exactly_one_match() {
    let spec = VolumeSpec {
        volume: Some(
            serde_json::from_value(json!({"name": "v", "hostPath": {"path": "/x"}})).unwrap(),
        ),
        ..Default::default()
    };
    let none: Vec<Arc<dyn VolumePlugin>> = vec![Arc::new(NfsPlugin)];
    assert_eq!(
        find_plugin_by_spec(&none, &spec).err().as_deref(),
        Some("no volume plugin matched")
    );
    let one: Vec<Arc<dyn VolumePlugin>> = vec![Arc::new(NfsPlugin), Arc::new(HostPathPlugin)];
    assert_eq!(
        find_plugin_by_spec(&one, &spec).unwrap().plugin_name(),
        "kubernetes.io/host-path"
    );
    let two: Vec<Arc<dyn VolumePlugin>> = vec![
        Arc::new(HostPathPlugin),
        Arc::new(FakePlugin {
            supports_selinux: false,
        }),
    ];
    assert_eq!(
        find_plugin_by_spec(&two, &spec).err().as_deref(),
        Some("multiple volume plugins matched: kubernetes.io/host-path,fake-plugin")
    );
}

#[test]
fn in_tree_plugin_volume_names_match_upstream_formats() {
    let pv = |spec: Value| VolumeSpec {
        persistent_volume: Some(
            serde_json::from_value(json!({"metadata": {"name": "pv"}, "spec": spec})).unwrap(),
        ),
        ..Default::default()
    };
    assert_eq!(
        NfsPlugin
            .get_volume_name(&pv(json!({"nfs": {"server": "s", "path": "/p"}})))
            .unwrap(),
        "s//p"
    );
    assert_eq!(
        IscsiPlugin
            .get_volume_name(&pv(
                json!({"iscsi": {"targetPortal": "tp", "iqn": "iqn", "lun": 3}})
            ))
            .unwrap(),
        "tp:iqn:3"
    );
    assert_eq!(
        FcPlugin
            .get_volume_name(&pv(json!({"fc": {"targetWWNs": ["a", "b"], "lun": 1}})))
            .unwrap(),
        "[a b]:1"
    );
    assert_eq!(
        FcPlugin
            .get_volume_name(&pv(json!({"fc": {"wwids": ["w1", "w2"]}})))
            .unwrap(),
        "[w1 w2]"
    );
    assert_eq!(
        LocalPlugin
            .get_volume_name(&pv(json!({"local": {"path": "/l"}})))
            .unwrap(),
        "pv"
    );
    let inline = VolumeSpec {
        volume: Some(
            serde_json::from_value(json!({"name": "v", "fc": {"wwids": ["w9"]}})).unwrap(),
        ),
        ..Default::default()
    };
    assert!(FcPlugin.can_support(&inline));
    assert_eq!(FcPlugin.get_volume_name(&inline).unwrap(), "[w9]");
}
