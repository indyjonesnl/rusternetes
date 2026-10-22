//! Port of `pkg/controller/volume/selinuxwarning/cache/conflict.go` and
//! `cache/volumecache.go` (release-1.35): every volume used by a Pod with the
//! properties the controller tracks (SELinux label, SELinuxChangePolicy), and
//! the conflicts between Pods that share a volume.

use super::translator::{parse_selinux_label, ControllerSELinuxTranslator};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::fmt;
use std::sync::RwLock;
use tracing::{enabled, info, Level};

/// `v1.SELinuxChangePolicyMountOption`.
pub const SELINUX_CHANGE_POLICY_MOUNT_OPTION: &str = "MountOption";
/// `v1.SELinuxChangePolicyRecursive`.
pub const SELINUX_CHANGE_POLICY_RECURSIVE: &str = "Recursive";

/// `cache.ObjectName` (client-go `tools/cache/object-names.go`): a
/// namespace/name pair, rendered `namespace/name`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct ObjectName {
    pub namespace: String,
    pub name: String,
}

impl ObjectName {
    pub fn new(namespace: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            name: name.into(),
        }
    }
}

impl fmt::Display for ObjectName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.namespace, self.name)
    }
}

/// `cache.Conflict`: a single conflict between two Pods using the same volume
/// with different SELinux labels or policies. The event is sent to both.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Conflict {
    /// Human-readable name of the conflicting property + value of the
    /// "property" label of the `selinux_volume_conflict` metric.
    pub property_name: String,
    /// Reason for the event (`Event.Reason`).
    pub event_reason: String,
    /// Pod to generate the event on.
    pub pod: ObjectName,
    pub property_value: String,
    /// Only for logging / messaging.
    pub other_pod: ObjectName,
    pub other_property_value: String,
}

impl Conflict {
    /// `Conflict.EventMessage`.
    pub fn event_message(&self) -> String {
        // Quote the values for better readability.
        let value = format!("\"{}\"", self.property_value);
        let other_value = format!("\"{}\"", self.other_property_value);
        if self.pod.namespace == self.other_pod.namespace {
            // In the same namespace, be very specific about the pod names.
            return format!(
                "{p} {value} conflicts with pod {other} that uses the same volume as this pod with {p} {other_value}. If both pods land on the same node, only one of them may access the volume.",
                p = self.property_name,
                other = self.other_pod.name,
            );
        }
        // Pods are in different namespaces, do not reveal the other namespace
        // or pod name.
        // (Upstream's `fmt.Sprint(c.PropertyName, value, ...)` has no space
        // between the property name and the value here; kept verbatim.)
        format!(
            "{p}{value} conflicts with another pod that uses the same volume as this pod with a different {p}. If both pods land on the same node, only one of them may access the volume.",
            p = self.property_name,
        )
    }
}

/// `cache.VolumeCache`.
pub trait VolumeCache: Send + Sync {
    /// Add a single volume to the cache. Returns the conflicts it caused.
    fn add_volume(
        &self,
        volume_name: &str,
        pod_key: &ObjectName,
        selinux_label: &str,
        change_policy: &str,
        csi_driver: &str,
    ) -> Vec<Conflict>;

    /// Remove a pod from the cache. Prunes all empty structures.
    fn delete_pod(&self, pod_key: &ObjectName);

    /// All pods that use volumes of the given CSI driver. Useful when a
    /// CSIDriver changes its `spec.seLinuxMount` and the controller needs to
    /// re-evaluate those pods. In-tree plugins are not tracked, because they
    /// do not change their SELinux support dynamically.
    fn get_pods_for_csi_driver(&self, driver_name: &str) -> Vec<ObjectName>;

    /// The current set of active conflicts (both directions).
    fn get_conflicts(&self) -> Vec<Conflict>;
}

/// `podInfo`: a Pod that uses a volume.
#[derive(Debug, Clone, PartialEq, Eq)]
struct PodInfo {
    /// SELinux label to be applied to the volume in the Pod, either as mount
    /// option or recursively by the container runtime.
    selinux_label: String,
    /// Pre-parsed label parts for fast conflict detection.
    selinux_parts: [String; 4],
    /// SELinuxChangePolicy of the Pod.
    change_policy: String,
}

impl PodInfo {
    fn new(label: &str, change_policy: &str) -> Self {
        Self {
            selinux_label: label.to_string(),
            selinux_parts: parse_selinux_label(label),
            change_policy: change_policy.to_string(),
        }
    }
}

/// `usedVolume`: a volume that is used by one or more existing pods.
#[derive(Debug)]
struct UsedVolume {
    csi_driver: String,
    /// Pods that use this volume, indexed by pod key for faster deletion.
    pods: HashMap<ObjectName, PodInfo>,
}

#[derive(Default)]
struct Inner {
    /// All volumes of all existing Pods.
    volumes: HashMap<String, UsedVolume>,
    /// Reverse index: each pod to the volumes it uses (used on pod deletion).
    pod_to_volumes: HashMap<ObjectName, HashSet<String>>,
    /// Currently active conflicts per volume (both directions, symmetric).
    conflicts: HashMap<String, Vec<Conflict>>,
}

/// `volumeCache` (`NewVolumeLabelCache`).
pub struct VolumeLabelCache {
    translator: ControllerSELinuxTranslator,
    inner: RwLock<Inner>,
}

impl VolumeLabelCache {
    /// `NewVolumeLabelCache`.
    pub fn new(translator: ControllerSELinuxTranslator) -> Self {
        Self {
            translator,
            inner: RwLock::new(Inner::default()),
        }
    }
}

impl Inner {
    /// `registerPodVolume`.
    fn register_pod_volume(&mut self, pod_key: &ObjectName, volume_name: &str) {
        self.pod_to_volumes
            .entry(pod_key.clone())
            .or_default()
            .insert(volume_name.to_string());
    }

    /// `volumeCache.dump` (logged at `dumpLogLevel = 10`, i.e. TRACE here).
    fn dump(&self) {
        if !enabled!(Level::TRACE) {
            return;
        }
        info!("VolumeCache dump:");
        let volume_ids: BTreeSet<&String> = self.volumes.keys().collect();
        for id in volume_ids {
            let volume = &self.volumes[id];
            info!(volume = %id, csi_driver = %volume.csi_driver, "Cached volume");
            let pods: BTreeSet<String> = volume.pods.keys().map(|k| k.to_string()).collect();
            for key in pods {
                let (ns, name) = key.split_once('/').unwrap_or(("", &key));
                let info_ = &volume.pods[&ObjectName::new(ns, name)];
                info!(pod = %key, selinux_label = %info_.selinux_label, change_policy = %info_.change_policy, "  pod");
            }
        }
        info!("VolumeCache reverse index dump:");
        let pods: BTreeSet<String> = self.pod_to_volumes.keys().map(|k| k.to_string()).collect();
        for key in pods {
            let (ns, name) = key.split_once('/').unwrap_or(("", &key));
            let vols: BTreeSet<&String> = self.pod_to_volumes[&ObjectName::new(ns, name)]
                .iter()
                .collect();
            info!(pod = %key, volumes = ?vols, "  pod");
        }
    }
}

impl VolumeCache for VolumeLabelCache {
    /// `volumeCache.AddVolume`.
    fn add_volume(
        &self,
        volume_name: &str,
        pod_key: &ObjectName,
        label: &str,
        change_policy: &str,
        csi_driver: &str,
    ) -> Vec<Conflict> {
        let mut inner = self.inner.write().expect("volume cache lock poisoned");
        let conflicts = add_volume_locked(
            &mut inner,
            &self.translator,
            volume_name,
            pod_key,
            label,
            change_policy,
            csi_driver,
        );
        inner.dump();
        conflicts
    }

    /// `volumeCache.DeletePod`.
    fn delete_pod(&self, pod_key: &ObjectName) {
        let mut inner = self.inner.write().expect("volume cache lock poisoned");
        let volume_names: Vec<String> = inner
            .pod_to_volumes
            .get(pod_key)
            .map(|v| v.iter().cloned().collect())
            .unwrap_or_default();

        for volume_name in &volume_names {
            let Some(conflicts) = inner.conflicts.get(volume_name) else {
                continue;
            };
            // Preserve other conflicts belonging to the volume.
            let updated: Vec<Conflict> = conflicts
                .iter()
                .filter(|c| &c.pod != pod_key && &c.other_pod != pod_key)
                .cloned()
                .collect();
            if updated.is_empty() {
                inner.conflicts.remove(volume_name);
            } else {
                inner.conflicts.insert(volume_name.clone(), updated);
            }
        }

        // Use the reverse index to only iterate through volumes this pod
        // actually uses.
        for volume_name in &volume_names {
            let Some(volume) = inner.volumes.get_mut(volume_name) else {
                continue;
            };
            volume.pods.remove(pod_key);
            if volume.pods.is_empty() {
                inner.volumes.remove(volume_name);
            }
        }
        inner.pod_to_volumes.remove(pod_key);
        inner.dump();
    }

    /// `volumeCache.GetPodsForCSIDriver`.
    fn get_pods_for_csi_driver(&self, driver_name: &str) -> Vec<ObjectName> {
        let inner = self.inner.read().expect("volume cache lock poisoned");
        let mut pods = Vec::new();
        for volume in inner.volumes.values() {
            if volume.csi_driver != driver_name {
                continue;
            }
            pods.extend(volume.pods.keys().cloned());
        }
        pods
    }

    /// `volumeCache.GetConflicts`: deduplicated across volumes.
    fn get_conflicts(&self) -> Vec<Conflict> {
        let inner = self.inner.read().expect("volume cache lock poisoned");
        tracing::debug!("Scraping conflicts");
        inner.dump();
        let mut result: HashSet<Conflict> = HashSet::new();
        for vol_conflicts in inner.conflicts.values() {
            result.extend(vol_conflicts.iter().cloned());
        }
        result.into_iter().collect()
    }
}

fn add_volume_locked(
    inner: &mut Inner,
    translator: &ControllerSELinuxTranslator,
    volume_name: &str,
    pod_key: &ObjectName,
    label: &str,
    change_policy: &str,
    csi_driver: &str,
) -> Vec<Conflict> {
    let mut conflicts = Vec::new();

    let Some(volume) = inner.volumes.get_mut(volume_name) else {
        // This is a new volume.
        let mut pods = HashMap::new();
        pods.insert(pod_key.clone(), PodInfo::new(label, change_policy));
        inner.volumes.insert(
            volume_name.to_string(),
            UsedVolume {
                csi_driver: csi_driver.to_string(),
                pods,
            },
        );
        // Add to reverse index.
        inner.register_pod_volume(pod_key, volume_name);
        return conflicts;
    };

    // The volume is already known.
    let pod_info = PodInfo::new(label, change_policy);
    if volume.pods.get(pod_key) == Some(&pod_info) {
        // The Pod is already known too and nothing changed since the last
        // update. All conflicts were already reported when the Pod was added /
        // updated in the cache last time.
        return conflicts;
    }

    // Add the updated pod info to the cache.
    volume.pods.insert(pod_key.clone(), pod_info.clone());

    // Emit conflicts for the pod.
    for (other_pod_key, other_pod_info) in &volume.pods {
        if other_pod_info.change_policy != change_policy {
            // Send conflict to both pods.
            conflicts.push(Conflict {
                property_name: "SELinuxChangePolicy".to_string(),
                event_reason: "SELinuxChangePolicyConflict".to_string(),
                pod: pod_key.clone(),
                property_value: change_policy.to_string(),
                other_pod: other_pod_key.clone(),
                other_property_value: other_pod_info.change_policy.clone(),
            });
            conflicts.push(Conflict {
                property_name: "SELinuxChangePolicy".to_string(),
                event_reason: "SELinuxChangePolicyConflict".to_string(),
                pod: other_pod_key.clone(),
                property_value: other_pod_info.change_policy.clone(),
                other_pod: pod_key.clone(),
                other_property_value: change_policy.to_string(),
            });
        }
        if translator.conflicts_parsed(&other_pod_info.selinux_parts, &pod_info.selinux_parts) {
            // Send conflict to both pods.
            conflicts.push(Conflict {
                property_name: "SELinuxLabel".to_string(),
                event_reason: "SELinuxLabelConflict".to_string(),
                pod: pod_key.clone(),
                property_value: label.to_string(),
                other_pod: other_pod_key.clone(),
                other_property_value: other_pod_info.selinux_label.clone(),
            });
            conflicts.push(Conflict {
                property_name: "SELinuxLabel".to_string(),
                event_reason: "SELinuxLabelConflict".to_string(),
                pod: other_pod_key.clone(),
                property_value: other_pod_info.selinux_label.clone(),
                other_pod: pod_key.clone(),
                other_property_value: label.to_string(),
            });
        }
    }

    // Add to reverse index.
    inner.register_pod_volume(pod_key, volume_name);

    // Update the conflict cache for this volume: remove stale conflicts for
    // this pod, then add new ones.
    let mut updated: Vec<Conflict> = inner
        .conflicts
        .get(volume_name)
        .map(|existing| {
            existing
                .iter()
                .filter(|c| &c.pod != pod_key && &c.other_pod != pod_key)
                .cloned()
                .collect()
        })
        .unwrap_or_default();
    updated.extend(conflicts.iter().cloned());
    if updated.is_empty() {
        inner.conflicts.remove(volume_name);
    } else {
        inner.conflicts.insert(volume_name.to_string(), updated);
    }

    conflicts
}

#[cfg(test)]
mod tests {
    //! Port of `cache/volumecache_test.go`: `TestVolumeCache_DeleteAll`,
    //! `TestVolumeCache_AddVolumeGetConflicts`,
    //! `TestVolumeCache_MultiVolumeConflicts`,
    //! `TestVolumeCache_DeletePodConflicts`,
    //! `TestVolumeCache_GetPodsForCSIDriver`.
    use super::*;

    const MOUNT: &str = SELINUX_CHANGE_POLICY_MOUNT_OPTION;
    const RECURSIVE: &str = SELINUX_CHANGE_POLICY_RECURSIVE;

    fn new_cache() -> VolumeLabelCache {
        VolumeLabelCache::new(ControllerSELinuxTranslator)
    }

    fn sort_conflicts(c: &mut [Conflict]) {
        c.sort_by(|a, b| {
            (a.pod.to_string(), a.other_pod.to_string())
                .cmp(&(b.pod.to_string(), b.other_pod.to_string()))
        });
    }

    /// `verifyReverseIndexConsistency`: the forward and reverse indexes are
    /// symmetric.
    fn verify_reverse_index_consistency(c: &VolumeLabelCache) {
        let inner = c.inner.read().unwrap();
        for (pod_key, volumes) in &inner.pod_to_volumes {
            for volume_name in volumes {
                let volume = inner.volumes.get(volume_name).unwrap_or_else(|| {
                    panic!(
                        "Reverse index has pod {pod_key} -> volume {volume_name}, but volume not in forward index"
                    )
                });
                assert!(
                    volume.pods.contains_key(pod_key),
                    "Reverse index has pod {pod_key} -> volume {volume_name}, but pod not in volume's pod list"
                );
            }
        }
        for (volume_name, volume) in &inner.volumes {
            for pod_key in volume.pods.keys() {
                let pod_volumes = inner.pod_to_volumes.get(pod_key).unwrap_or_else(|| {
                    panic!(
                        "Forward index has volume {volume_name} -> pod {pod_key}, but pod not in reverse index"
                    )
                });
                assert!(
                    pod_volumes.contains(volume_name),
                    "Forward index has volume {volume_name} -> pod {pod_key}, but volume not in pod's volume list"
                );
            }
        }
    }

    /// Delete all items in a bigger cache and check it's empty.
    #[test]
    fn delete_all() {
        let c = new_cache();
        let mut pods_to_delete = Vec::new();
        for ns in ["ns1", "ns2", "ns3", "ns4"] {
            for name in ["pod1", "pod2", "pod3", "pod4"] {
                for vol in ["vol1", "vol2", "vol3", "vol4"] {
                    let key = ObjectName::new(ns, name);
                    pods_to_delete.push(key.clone());
                    let conflicts = c.add_volume(vol, &key, "label1", MOUNT, "csiDriver1");
                    // All volumes have the same labels and policy, there
                    // should be no conflicts.
                    assert!(conflicts.is_empty(), "{ns}/{name} {vol}: {conflicts:?}");
                }
            }
        }
        verify_reverse_index_consistency(&c);
        for key in &pods_to_delete {
            c.delete_pod(key);
        }
        let inner = c.inner.read().unwrap();
        assert!(inner.volumes.is_empty(), "cache must be empty");
        assert!(
            inner.pod_to_volumes.is_empty(),
            "reverse index must be empty"
        );
        drop(inner);
        verify_reverse_index_consistency(&c);
    }

    #[derive(Clone)]
    struct PodWithVolume {
        ns: &'static str,
        name: &'static str,
        volume: &'static str,
        label: &'static str,
        policy: &'static str,
    }

    fn pwv(
        ns: &'static str,
        name: &'static str,
        volume: &'static str,
        label: &'static str,
        policy: &'static str,
    ) -> PodWithVolume {
        PodWithVolume {
            ns,
            name,
            volume,
            label,
            policy,
        }
    }

    fn add_reverse_conflict(conflicts: Vec<Conflict>) -> Vec<Conflict> {
        let mut out = Vec::with_capacity(conflicts.len() * 2);
        for c in conflicts {
            let reversed = Conflict {
                property_name: c.property_name.clone(),
                event_reason: c.event_reason.clone(),
                pod: c.other_pod.clone(),
                property_value: c.other_property_value.clone(),
                other_pod: c.pod.clone(),
                other_property_value: c.property_value.clone(),
            };
            out.push(c);
            out.push(reversed);
        }
        out
    }

    fn label_conflict(
        pod: (&str, &str),
        value: &str,
        other: (&str, &str),
        other_value: &str,
    ) -> Conflict {
        Conflict {
            property_name: "SELinuxLabel".into(),
            event_reason: "SELinuxLabelConflict".into(),
            pod: ObjectName::new(pod.0, pod.1),
            property_value: value.into(),
            other_pod: ObjectName::new(other.0, other.1),
            other_property_value: other_value.into(),
        }
    }

    fn policy_conflict(
        pod: (&str, &str),
        value: &str,
        other: (&str, &str),
        other_value: &str,
    ) -> Conflict {
        Conflict {
            property_name: "SELinuxChangePolicy".into(),
            event_reason: "SELinuxChangePolicyConflict".into(),
            pod: ObjectName::new(pod.0, pod.1),
            property_value: value.into(),
            other_pod: ObjectName::new(other.0, other.1),
            other_property_value: other_value.into(),
        }
    }

    /// AddVolume and GetConflicts return the same conflict data.
    #[test]
    fn add_volume_get_conflicts() {
        let existing = vec![
            pwv(
                "ns1",
                "pod1-mountOption",
                "vol1",
                "system_u:system_r:label1",
                MOUNT,
            ),
            // Labels on volumes with Recursive policy are cleared, we don't
            // want the controller to report label conflicts on them.
            pwv("ns2", "pod2-recursive", "vol2", "", RECURSIVE),
            // vol3 is used by 2 pods with the same label + recursive policy.
            pwv("ns3", "pod3-1", "vol3", "", RECURSIVE),
            pwv("ns3", "pod3-2", "vol3", "", RECURSIVE),
            // vol4 is used by 2 pods with the same label + mount policy.
            pwv("ns4", "pod4-1", "vol4", "system_u:system_r:label4", MOUNT),
            pwv("ns4", "pod4-2", "vol4", "system_u:system_r:label4", MOUNT),
            // vol5 has no user and role.
            pwv("ns5", "pod5", "vol5", "::label5", MOUNT),
            // vol6 has no user.
            pwv("ns6", "pod6", "vol6", ":system_r:label6", MOUNT),
            // vol7 has no user and role, but has categories.
            pwv("ns7", "pod7", "vol7", "::label7:c0,c1", MOUNT),
            // vol8 has no label.
            pwv("ns8", "pod8", "vol8", "", MOUNT),
        ];

        struct Case {
            name: &'static str,
            initial: Vec<PodWithVolume>,
            to_add: PodWithVolume,
            expected: Vec<Conflict>,
        }
        let t = ("testns", "testpod");
        let cases = vec![
            Case {
                name: "new volume in empty cache",
                initial: vec![],
                to_add: pwv("testns", "testpod", "vol-new", "system_u:system_r:label-new", MOUNT),
                expected: vec![],
            },
            Case {
                name: "new volume",
                initial: existing.clone(),
                to_add: pwv("testns", "testpod", "vol-new", "system_u:system_r:label-new", MOUNT),
                expected: vec![],
            },
            Case {
                name: "existing volume in a new pod with existing policy and label",
                initial: existing.clone(),
                to_add: pwv("testns", "testpod", "vol1", "system_u:system_r:label1", MOUNT),
                expected: vec![],
            },
            Case {
                name: "existing volume in a new pod with existing policy and new conflicting label",
                initial: existing.clone(),
                to_add: pwv("testns", "testpod", "vol1", "system_u:system_r:label-new", MOUNT),
                expected: vec![label_conflict(
                    t,
                    "system_u:system_r:label-new",
                    ("ns1", "pod1-mountOption"),
                    "system_u:system_r:label1",
                )],
            },
            Case {
                name: "existing volume in a new pod with new conflicting policy",
                initial: existing.clone(),
                to_add: pwv("testns", "testpod", "vol1", "", RECURSIVE),
                expected: vec![policy_conflict(
                    t,
                    "Recursive",
                    ("ns1", "pod1-mountOption"),
                    "MountOption",
                )],
            },
            Case {
                name: "existing pod is replaced with different non-conflicting policy and label",
                initial: existing.clone(),
                // There is no other pod that uses vol2 -> change of policy
                // and label is possible.
                to_add: pwv("ns2", "pod2-recursive", "vol2", "system_u:system_r:label-new", MOUNT),
                expected: vec![],
            },
            Case {
                name: "existing pod is replaced with conflicting policy",
                initial: existing.clone(),
                // vol3 is used by pod3-2 with Recursive policy. A policy
                // change can happen when a CSIDriver is updated from
                // SELinuxMount: false to SELinuxMount: true.
                to_add: pwv("ns3", "pod3-1", "vol3", "system_u:system_r:label-new", MOUNT),
                expected: vec![policy_conflict(
                    ("ns3", "pod3-1"),
                    "MountOption",
                    ("ns3", "pod3-2"),
                    "Recursive",
                )],
            },
            Case {
                name: "existing volume in a new pod with existing policy and new incomparable label (missing user and role)",
                initial: existing.clone(),
                to_add: pwv("testns", "testpod", "vol5", "system_u:system_r:label5", MOUNT),
                expected: vec![],
            },
            Case {
                name: "existing volume in a new pod with conflicting policy with incomparable parts",
                initial: existing.clone(),
                to_add: pwv("testns", "testpod", "vol5", "::label6", MOUNT),
                expected: vec![label_conflict(t, "::label6", ("ns5", "pod5"), "::label5")],
            },
            Case {
                name: "existing volume in a new pod with existing policy and new incomparable label (missing user)",
                initial: existing.clone(),
                to_add: pwv("testns", "testpod", "vol6", "system_u::label6", MOUNT),
                expected: vec![],
            },
            Case {
                name: "existing volume in a new pod with existing policy and new comparable label (missing categories)",
                initial: existing.clone(),
                to_add: pwv("testns", "testpod", "vol7", "system_u:system_r:label7", MOUNT),
                expected: vec![label_conflict(
                    t,
                    "system_u:system_r:label7",
                    ("ns7", "pod7"),
                    "::label7:c0,c1",
                )],
            },
            Case {
                name: "existing volume in a new pod with existing policy and new incomparable label (missing everything)",
                initial: existing.clone(),
                to_add: pwv("testns", "testpod", "vol8", "system_u:system_r:label8", MOUNT),
                expected: vec![],
            },
            Case {
                name: "existing volume in a new pod with existing policy and new comparable label (missing everything but categories)",
                initial: existing.clone(),
                to_add: pwv("testns", "testpod", "vol8", "system_u:system_r:label8:c0,c1", MOUNT),
                expected: vec![label_conflict(
                    t,
                    "system_u:system_r:label8:c0,c1",
                    ("ns8", "pod8"),
                    "",
                )],
            },
        ];

        for tt in cases {
            let c = new_cache();
            for p in &tt.initial {
                let conflicts = c.add_volume(
                    p.volume,
                    &ObjectName::new(p.ns, p.name),
                    p.label,
                    p.policy,
                    "csiDriver1",
                );
                assert!(
                    conflicts.is_empty(),
                    "{}: initial AddVolume {}/{} {} returned conflicts: {conflicts:?}",
                    tt.name,
                    p.ns,
                    p.name,
                    p.volume
                );
            }

            let p = &tt.to_add;
            let pod_key = ObjectName::new(p.ns, p.name);
            let mut conflicts = c.add_volume(p.volume, &pod_key, p.label, p.policy, "csiDriver1");

            let mut expected = add_reverse_conflict(tt.expected.clone());
            sort_conflicts(&mut conflicts);
            sort_conflicts(&mut expected);
            assert_eq!(conflicts, expected, "{}: AddVolume conflicts", tt.name);

            // Expect the pod + volume to be present in the cache.
            {
                let inner = c.inner.read().unwrap();
                let volume = inner
                    .volumes
                    .get(p.volume)
                    .unwrap_or_else(|| panic!("{}: volume missing", tt.name));
                let existing_info = volume
                    .pods
                    .get(&pod_key)
                    .unwrap_or_else(|| panic!("{}: pod missing", tt.name));
                assert_eq!(
                    existing_info,
                    &PodInfo::new(p.label, p.policy),
                    "{}: pod info",
                    tt.name
                );
            }

            verify_reverse_index_consistency(&c);

            // GetConflicts returns the same conflicts.
            let mut received = c.get_conflicts();
            sort_conflicts(&mut received);
            assert_eq!(received, expected, "{}: GetConflicts", tt.name);
        }
    }

    /// Conflicts are tracked per-volume: a pod with conflicts on multiple
    /// volumes retains all of them after successive AddVolume calls.
    #[test]
    fn multi_volume_conflicts() {
        let c = new_cache();
        let pod_a = ObjectName::new("ns", "podA");
        let pod_b = ObjectName::new("ns", "podB");
        let pod_c = ObjectName::new("ns", "podC");

        c.add_volume("vol1", &pod_b, "system_u:system_r:labelB", MOUNT, "driver1");
        c.add_volume("vol2", &pod_c, "system_u:system_r:labelC", MOUNT, "driver1");

        // podA uses vol1 with a different label (conflict with podB).
        let conflicts1 = c.add_volume("vol1", &pod_a, "system_u:system_r:labelA", MOUNT, "driver1");
        assert!(!conflicts1.is_empty(), "expected conflicts on vol1");
        // podA also uses vol2 with a different label (conflict with podC).
        let conflicts2 = c.add_volume("vol2", &pod_a, "system_u:system_r:labelA", MOUNT, "driver1");
        assert!(!conflicts2.is_empty(), "expected conflicts on vol2");

        // GetConflicts must return conflicts from BOTH volumes.
        let all = c.get_conflicts();
        assert_eq!(all.len(), conflicts1.len() + conflicts2.len());

        // After deleting podA, all conflicts should be gone.
        c.delete_pod(&pod_a);
        assert!(c.get_conflicts().is_empty());

        // Deduplication: podD and podE conflict on two volumes with the same
        // labels. Identical Conflict entries from different volumes must be
        // deduplicated by GetConflicts.
        let pod_d = ObjectName::new("ns", "podD");
        let pod_e = ObjectName::new("ns", "podE");
        c.add_volume("vol3", &pod_d, "system_u:system_r:labelD", MOUNT, "driver1");
        c.add_volume("vol4", &pod_d, "system_u:system_r:labelD", MOUNT, "driver1");
        let conflicts_vol3 =
            c.add_volume("vol3", &pod_e, "system_u:system_r:labelE", MOUNT, "driver1");
        let conflicts_vol4 =
            c.add_volume("vol4", &pod_e, "system_u:system_r:labelE", MOUNT, "driver1");
        assert_eq!(conflicts_vol3.len(), conflicts_vol4.len());
        assert!(!conflicts_vol3.is_empty());

        let de_count = c
            .get_conflicts()
            .iter()
            .filter(|x| {
                x.pod == pod_d || x.pod == pod_e || x.other_pod == pod_d || x.other_pod == pod_e
            })
            .count();
        assert_eq!(de_count, conflicts_vol3.len());
    }

    #[test]
    fn delete_pod_conflicts() {
        let a = ObjectName::new("ns", "podA");
        let b = ObjectName::new("ns", "podB");
        let cc = ObjectName::new("ns", "podC");
        let d = ObjectName::new("ns", "podD");

        struct Case {
            name: &'static str,
            initial: Vec<PodWithVolume>,
            to_delete: ObjectName,
            delete_twice: bool,
            surviving: Vec<(ObjectName, ObjectName)>,
        }
        let la = "system_u:system_r:labelA";
        let lb = "system_u:system_r:labelB";
        let lc = "system_u:system_r:labelC";
        let ld = "system_u:system_r:labelD";
        let cases = vec![
            Case {
                name: "delete one of two conflicting pods clears all conflicts",
                initial: vec![
                    pwv("ns", "podA", "vol1", la, MOUNT),
                    pwv("ns", "podB", "vol1", lb, MOUNT),
                ],
                to_delete: a.clone(),
                delete_twice: false,
                surviving: vec![],
            },
            Case {
                name: "delete non-conflicting pod preserves existing conflicts",
                initial: vec![
                    pwv("ns", "podA", "vol1", la, MOUNT),
                    pwv("ns", "podB", "vol1", lb, MOUNT),
                    pwv("ns", "podC", "vol2", lc, MOUNT),
                ],
                to_delete: cc.clone(),
                delete_twice: false,
                surviving: vec![(a.clone(), b.clone())],
            },
            Case {
                name: "three pods on same volume delete one leaves remaining pair conflict",
                initial: vec![
                    pwv("ns", "podA", "vol1", la, MOUNT),
                    pwv("ns", "podB", "vol1", lb, MOUNT),
                    pwv("ns", "podC", "vol1", lc, MOUNT),
                ],
                to_delete: a.clone(),
                delete_twice: false,
                surviving: vec![(b.clone(), cc.clone())],
            },
            Case {
                name: "delete pod with conflicts on multiple volumes",
                initial: vec![
                    pwv("ns", "podB", "vol1", lb, MOUNT),
                    pwv("ns", "podC", "vol2", lc, MOUNT),
                    pwv("ns", "podA", "vol1", la, MOUNT),
                    pwv("ns", "podA", "vol2", la, MOUNT),
                ],
                to_delete: a.clone(),
                delete_twice: false,
                surviving: vec![],
            },
            Case {
                name: "delete pod preserves conflicts on unrelated volumes",
                initial: vec![
                    pwv("ns", "podA", "vol1", la, MOUNT),
                    pwv("ns", "podB", "vol1", lb, MOUNT),
                    pwv("ns", "podC", "vol2", lc, MOUNT),
                    pwv("ns", "podD", "vol2", ld, MOUNT),
                ],
                to_delete: a.clone(),
                delete_twice: false,
                surviving: vec![(cc.clone(), d.clone())],
            },
            Case {
                name: "delete pod that was already deleted is a no-op",
                initial: vec![
                    pwv("ns", "podA", "vol1", la, MOUNT),
                    pwv("ns", "podB", "vol1", lb, MOUNT),
                ],
                to_delete: a.clone(),
                delete_twice: true,
                surviving: vec![],
            },
        ];

        for tt in cases {
            let c = new_cache();
            for p in &tt.initial {
                c.add_volume(
                    p.volume,
                    &ObjectName::new(p.ns, p.name),
                    p.label,
                    p.policy,
                    "driver1",
                );
            }
            c.delete_pod(&tt.to_delete);
            if tt.delete_twice {
                c.delete_pod(&tt.to_delete);
            }
            let remaining = c.get_conflicts();

            // Deleted pod must not appear in any conflict.
            for conflict in &remaining {
                assert!(
                    conflict.pod != tt.to_delete && conflict.other_pod != tt.to_delete,
                    "{}: conflict involving deleted pod: {conflict:?}",
                    tt.name
                );
            }
            // Each expected surviving pair exists in both directions.
            for (p0, p1) in &tt.surviving {
                let forward = remaining.iter().any(|x| &x.pod == p0 && &x.other_pod == p1);
                let reverse = remaining.iter().any(|x| &x.pod == p1 && &x.other_pod == p0);
                assert!(
                    forward && reverse,
                    "{}: expected symmetric conflict between {p0} and {p1}, got {remaining:?}",
                    tt.name
                );
            }
            if tt.surviving.is_empty() {
                assert!(remaining.is_empty(), "{}: {remaining:?}", tt.name);
            }
            verify_reverse_index_consistency(&c);
        }
    }

    #[test]
    fn get_pods_for_csi_driver() {
        let c = new_cache();
        let existing: Vec<(&str, Vec<PodWithVolume>)> = vec![
            (
                "csiDriver1",
                vec![pwv("ns1", "pod1-mountOption", "vol1", "label1", MOUNT)],
            ),
            (
                "csiDriver2",
                vec![
                    pwv("ns2", "pod2-recursive", "vol2", "label2", RECURSIVE),
                    // vol3 is used by 2 pods with the same label + recursive policy.
                    pwv("ns3", "pod3-1", "vol3", "label3", RECURSIVE),
                    pwv("ns3", "pod3-2", "vol3", "label3", RECURSIVE),
                ],
            ),
            (
                "csiDriver3",
                vec![
                    // vol4 is used by 2 pods with the same label + mount policy.
                    pwv("ns4", "pod4-1", "vol4", "label4", MOUNT),
                    pwv("ns4", "pod4-2", "vol4", "label4", MOUNT),
                ],
            ),
        ];
        for (driver, pods) in &existing {
            for p in pods {
                let conflicts = c.add_volume(
                    p.volume,
                    &ObjectName::new(p.ns, p.name),
                    p.label,
                    p.policy,
                    driver,
                );
                assert!(conflicts.is_empty(), "{conflicts:?}");
            }
        }

        let expected: Vec<(&str, Vec<ObjectName>)> = vec![
            (
                "csiDriver1",
                vec![ObjectName::new("ns1", "pod1-mountOption")],
            ),
            (
                "csiDriver2",
                vec![
                    ObjectName::new("ns2", "pod2-recursive"),
                    ObjectName::new("ns3", "pod3-1"),
                    ObjectName::new("ns3", "pod3-2"),
                ],
            ),
            (
                "csiDriver3",
                vec![
                    ObjectName::new("ns4", "pod4-1"),
                    ObjectName::new("ns4", "pod4-2"),
                ],
            ),
            // Totally unknown CSI driver.
            ("csiDriver4", vec![]),
        ];
        for (driver, want) in expected {
            let mut got = c.get_pods_for_csi_driver(driver);
            got.sort_by_key(|k| k.to_string());
            assert_eq!(got, want, "GetPodsForCSIDriver({driver})");
        }
    }

    /// `Conflict.EventMessage` (conflict.go), both namespace branches.
    #[test]
    fn event_message_matches_upstream_format() {
        let same_ns = label_conflict(("ns", "p1"), "a", ("ns", "p2"), "b");
        assert_eq!(
            same_ns.event_message(),
            "SELinuxLabel \"a\" conflicts with pod p2 that uses the same volume as this pod with SELinuxLabel \"b\". If both pods land on the same node, only one of them may access the volume."
        );
        let other_ns = label_conflict(("ns", "p1"), "a", ("ns2", "p2"), "b");
        assert_eq!(
            other_ns.event_message(),
            "SELinuxLabel\"a\" conflicts with another pod that uses the same volume as this pod with a different SELinuxLabel. If both pods land on the same node, only one of them may access the volume."
        );
    }
}
