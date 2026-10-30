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
pub struct PodInfo {
    /// SELinux label to be applied to the volume in the Pod, either as mount
    /// option or recursively by the container runtime.
    selinux_label: String,
    /// Pre-parsed label parts for fast conflict detection.
    selinux_parts: [String; 4],
    /// SELinuxChangePolicy of the Pod.
    change_policy: String,
}

impl PodInfo {
    pub fn new(label: &str, change_policy: &str) -> Self {
        Self {
            selinux_label: label.to_string(),
            selinux_parts: parse_selinux_label(label),
            change_policy: change_policy.to_string(),
        }
    }
}

/// `usedVolume`: a volume that is used by one or more existing pods.
#[derive(Debug)]
pub struct UsedVolume {
    pub csi_driver: String,
    /// Pods that use this volume, indexed by pod key for faster deletion.
    pub pods: HashMap<ObjectName, PodInfo>,
}

#[derive(Default)]
pub struct Inner {
    /// All volumes of all existing Pods.
    pub volumes: HashMap<String, UsedVolume>,
    /// Reverse index: each pod to the volumes it uses (used on pod deletion).
    pub pod_to_volumes: HashMap<ObjectName, HashSet<String>>,
    /// Currently active conflicts per volume (both directions, symmetric).
    pub conflicts: HashMap<String, Vec<Conflict>>,
}

/// `volumeCache` (`NewVolumeLabelCache`).
pub struct VolumeLabelCache {
    pub translator: ControllerSELinuxTranslator,
    pub inner: RwLock<Inner>,
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
