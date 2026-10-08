//! Volume recycler: the recycler pod template of the in-tree hostPath and NFS
//! plugins plus the pod-watching client that runs it.
//!
//! Ported from:
//! - `pkg/volume/util/recyclerclient/recycler_client.go`
//!   (`RecycleVolumeByWatchingPodUntilCompletion`, `waitForPod`, `recyclerClient`)
//! - `pkg/volume/plugins.go:978-1014` (`NewPersistentVolumeRecyclerPodTemplate`)
//! - `pkg/volume/hostpath/host_path.go:156-171` and
//!   `pkg/volume/nfs/nfs.go:156-171` (the two plugins' `Recycle`)
//! - `pkg/volume/util/util.go:152-162` (`CalculateTimeoutForVolume`)
//! - `pkg/controller/volume/persistentvolume/config/v1alpha1/defaults.go:81-91`
//!   (the timeout defaults)

use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::StreamExt;
use rusternetes_common::resources::{Event, PersistentVolume, Pod};
use rusternetes_common::types::Phase;
use rusternetes_storage::{build_key, Storage, WatchEvent};
use std::sync::Arc;

/// `MinimumTimeoutHostPath` default (`defaults.go:87-89`).
pub const MINIMUM_TIMEOUT_HOST_PATH: i64 = 60;
/// `IncrementTimeoutHostPath` default (`defaults.go:90-92`).
pub const INCREMENT_TIMEOUT_HOST_PATH: i64 = 30;
/// `MinimumTimeoutNFS` default (`defaults.go:81-83`).
pub const MINIMUM_TIMEOUT_NFS: i64 = 300;
/// `IncrementTimeoutNFS` default (`defaults.go:84-86`).
pub const INCREMENT_TIMEOUT_NFS: i64 = 30;

/// `CalculateTimeoutForVolume` (`pkg/volume/util/util.go:152-162`): the
/// greater of `minimum_timeout` and `increment` seconds per whole Gi of the
/// PV's storage capacity.
pub fn calculate_timeout_for_volume(
    minimum_timeout: i64,
    increment: i64,
    pv: &PersistentVolume,
) -> i64 {
    const GI: i128 = 1024 * 1024 * 1024;
    let size_bytes = pv
        .spec
        .capacity
        .get("storage")
        .and_then(|q| quantity_bytes(q))
        .unwrap_or(0);
    let timeout = (size_bytes / GI) as i64 * increment;
    if timeout < minimum_timeout {
        minimum_timeout
    } else {
        timeout
    }
}

/// `Quantity.Value()` for a storage quantity: whole bytes, rounded up like
/// upstream's `Value()`.
fn quantity_bytes(q: &str) -> Option<i128> {
    let q = q.trim();
    let split = q
        .find(|c: char| !(c.is_ascii_digit() || c == '.'))
        .unwrap_or(q.len());
    let (num, suffix) = q.split_at(split);
    let (int_s, frac_s) = num.split_once('.').unwrap_or((num, ""));
    if int_s.is_empty() && frac_s.is_empty() {
        return None;
    }
    let digits: i128 = format!("{int_s}{frac_s}").parse().ok()?;
    let scale = 10i128.checked_pow(u32::try_from(frac_s.len()).ok()?)?;
    let mult: i128 = match suffix {
        "" => 1,
        "k" => 10i128.pow(3),
        "M" => 10i128.pow(6),
        "G" => 10i128.pow(9),
        "T" => 10i128.pow(12),
        "P" => 10i128.pow(15),
        "E" => 10i128.pow(18),
        "Ki" => 1 << 10,
        "Mi" => 1 << 20,
        "Gi" => 1 << 30,
        "Ti" => 1 << 40,
        "Pi" => 1 << 50,
        "Ei" => 1 << 60,
        "m" => return Some((digits + 999) / 1000 / scale),
        exp => {
            let e: i32 = exp.strip_prefix(['e', 'E'])?.parse().ok()?;
            if !(0..=18).contains(&e) {
                return None;
            }
            10i128.pow(e as u32)
        }
    };
    let total = digits.checked_mul(mult)?;
    Some((total + scale - 1) / scale)
}

/// `NewPersistentVolumeRecyclerPodTemplate` (`pkg/volume/plugins.go:978-1014`).
/// The volume source of `volumes[0]` is left empty: "IMPORTANT! All plugins
/// using this template MUST override pod.Spec.Volumes[0].VolumeSource".
pub fn new_persistent_volume_recycler_pod_template() -> Pod {
    serde_json::from_value(serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "generateName": "pv-recycler-",
            "namespace": "default",
        },
        "spec": {
            "activeDeadlineSeconds": 60,
            "restartPolicy": "Never",
            "volumes": [{"name": "vol"}],
            "containers": [{
                "name": "pv-recycler",
                "image": "registry.k8s.io/build-image/debian-base:bookworm-v1.0.6",
                "command": ["/bin/sh"],
                "args": [
                    "-c",
                    "test -e /scrub && find /scrub -mindepth 1 -delete && test -z \"$(ls -A /scrub)\" || exit 1"
                ],
                "volumeMounts": [{"name": "vol", "mountPath": "/scrub"}],
            }],
        },
    }))
    .expect("recycler pod template is a valid Pod")
}

/// `hostPathPlugin.Recycle` pod construction (`host_path.go:156-169`).
pub fn host_path_recycler_pod(pv: &PersistentVolume) -> Result<Pod, String> {
    use rusternetes_common::resources::pod::HostPathVolumeSource;
    let Some(hp) = pv.spec.host_path.as_ref() else {
        return Err("spec.PersistentVolume.Spec.HostPath is nil".to_string());
    };
    let mut pod = new_persistent_volume_recycler_pod_template();
    let timeout =
        calculate_timeout_for_volume(MINIMUM_TIMEOUT_HOST_PATH, INCREMENT_TIMEOUT_HOST_PATH, pv);
    let spec = pod.spec.as_mut().expect("template has a spec");
    spec.active_deadline_seconds = Some(timeout);
    spec.volumes.as_mut().unwrap()[0].host_path = Some(HostPathVolumeSource {
        path: hp.path.clone(),
        type_: None,
    });
    Ok(pod)
}

/// `nfsPlugin.Recycle` pod construction (`nfs.go:156-171`).
pub fn nfs_recycler_pod(pv: &PersistentVolume) -> Result<Pod, String> {
    let Some(nfs) = pv.spec.nfs.as_ref() else {
        return Err("spec.PersistentVolumeSource.NFS is nil".to_string());
    };
    let mut pod = new_persistent_volume_recycler_pod_template();
    let timeout = calculate_timeout_for_volume(MINIMUM_TIMEOUT_NFS, INCREMENT_TIMEOUT_NFS, pv);
    pod.metadata.generate_name = Some("pv-recycler-nfs-".to_string());
    let spec = pod.spec.as_mut().expect("template has a spec");
    spec.active_deadline_seconds = Some(timeout);
    spec.volumes.as_mut().unwrap()[0].nfs = Some(nfs.clone());
    Ok(pod)
}

/// `FindRecyclablePluginBySpec` + `plugin.Recycle`'s pod construction: hostPath
/// first, then NFS (the two recyclable plugins registered at
/// `cmd/kube-controller-manager/app/plugins.go:67-120`). `None` means no
/// recycler plugin matches.
pub fn recycler_pod_for_volume(pv: &PersistentVolume) -> Option<Result<Pod, String>> {
    if pv.spec.host_path.is_some() {
        Some(host_path_recycler_pod(pv))
    } else if pv.spec.nfs.is_some() {
        Some(nfs_recycler_pod(pv))
    } else {
        None
    }
}

/// An item of the stream `recyclerClient.WatchPod` returns: a change of the
/// recycler pod or an event about it (`recycler_client.go:113-151`).
#[derive(Debug, Clone)]
pub enum RecyclerWatchEvent {
    PodAdded(Pod),
    PodModified(Pod),
    PodDeleted,
    /// `watch.Error` on a pod object.
    PodError,
    /// An `Added` `v1.Event` about the pod.
    Event {
        event_type: String,
        message: String,
    },
}

/// Failure of `recyclerClient.CreatePod`.
#[derive(Debug)]
pub enum CreatePodError {
    AlreadyExists,
    Other(String),
}

/// `recyclerClient` (`recycler_client.go:157-167`): a narrower pod API that
/// eases testing.
#[async_trait]
pub trait RecyclerClient: Send + Sync {
    async fn create_pod(&self, pod: &Pod) -> Result<(), CreatePodError>;
    async fn delete_pod(&self, name: &str, namespace: &str) -> Result<(), String>;
    /// Watches the pod and events related to it.
    async fn watch_pod(
        &self,
        name: &str,
        namespace: &str,
    ) -> Result<BoxStream<'static, RecyclerWatchEvent>, String>;
    /// Sends an event to the volume that is being recycled.
    fn event(&self, event_type: &str, message: &str);
}

/// `internalRecycleVolumeByWatchingPodUntilCompletion`
/// (`recycler_client.go:56-109`). Saves the pod to the API and watches it
/// until it completes, fails, or the pod's `activeDeadlineSeconds` is
/// exceeded; the recycler pod is always deleted before returning.
///
/// A pod with the same name that already exists is deleted ("it is not able to
/// judge if it is an old recycler or user has forged a fake recycler to block
/// Kubernetes from recycling") and the recycle retried later.
pub async fn recycle_volume_by_watching_pod_until_completion(
    pv_name: &str,
    mut pod: Pod,
    client: &dyn RecyclerClient,
) -> Result<(), String> {
    // Generate unique name for the recycler pod - we need to get "already
    // exists" error when a previous controller has already started recycling
    // the volume. Here we assume that pv.Name is already unique.
    pod.metadata.name = format!("recycler-for-{pv_name}");
    pod.metadata.generate_name = None;
    let namespace = pod
        .metadata
        .namespace
        .clone()
        .unwrap_or_else(|| "default".to_string());

    let mut pod_ch = match client.watch_pod(&pod.metadata.name, &namespace).await {
        Ok(ch) => ch,
        Err(e) => {
            tracing::debug!(
                "cannot start watcher for pod {}/{}: {}",
                namespace,
                pod.metadata.name,
                e
            );
            return Err(e);
        }
    };

    // Start the pod
    if let Err(e) = client.create_pod(&pod).await {
        return match e {
            CreatePodError::AlreadyExists => {
                if let Err(delete_err) = client.delete_pod(&pod.metadata.name, &namespace).await {
                    return Err(format!(
                        "failed to delete old recycler pod {}/{}: {}",
                        namespace, pod.metadata.name, delete_err
                    ));
                }
                // Recycler will try again and the old pod will be hopefully
                // deleted at that time.
                Err("old recycler pod found, will retry later".to_string())
            }
            CreatePodError::Other(e) => {
                Err(format!("unexpected error creating recycler pod:  {e}"))
            }
        };
    }
    let result = wait_for_pod(&pod, client, &mut pod_ch).await;

    // In all cases delete the recycler pod and log its result.
    tracing::info!("deleting recycler pod {}/{}", namespace, pod.metadata.name);
    let delete_err = client.delete_pod(&pod.metadata.name, &namespace).await;
    if let Err(e) = &delete_err {
        tracing::error!(
            "failed to delete recycler pod {}/{}: {}",
            namespace,
            pod.metadata.name,
            e
        );
    }

    // Returning recycler error is preferred, the pod will be deleted again on
    // the next retry.
    if let Err(e) = result {
        return Err(format!("failed to recycle volume: {e}"));
    }

    // Recycle succeeded but we failed to delete the recycler pod. Report it,
    // the controller will re-try recycling the PV again shortly.
    if let Err(e) = delete_err {
        return Err(format!("failed to delete recycler pod: {e}"));
    }
    Ok(())
}

/// `waitForPod` (`recycler_client.go:113-153`): watches the pod until it
/// finishes and sends all events on the pod to the PV.
async fn wait_for_pod(
    pod: &Pod,
    client: &dyn RecyclerClient,
    pod_ch: &mut BoxStream<'static, RecyclerWatchEvent>,
) -> Result<(), String> {
    loop {
        let Some(event) = pod_ch.next().await else {
            return Err(format!(
                "recycler pod {:?} watch channel had been closed",
                pod.metadata.name
            ));
        };
        match event {
            RecyclerWatchEvent::PodAdded(p) | RecyclerWatchEvent::PodModified(p) => {
                let status = p.status.as_ref();
                match status.and_then(|s| s.phase.clone()) {
                    Some(Phase::Succeeded) => return Ok(()),
                    Some(Phase::Failed) => {
                        return match status.and_then(|s| s.message.as_deref()) {
                            Some(m) if !m.is_empty() => Err(m.to_string()),
                            _ => Err("pod failed, pod.Status.Message unknown".to_string()),
                        };
                    }
                    _ => {}
                }
            }
            RecyclerWatchEvent::PodDeleted => {
                return Err("recycler pod was deleted".to_string());
            }
            RecyclerWatchEvent::PodError => {
                return Err("recycler pod watcher failed".to_string());
            }
            RecyclerWatchEvent::Event {
                event_type,
                message,
            } => client.event(&event_type, &message),
        }
    }
}

/// `RecycleEventRecorder` (`recycler_client.go:35`).
pub type RecycleEventRecorder = Box<dyn Fn(&str, &str) + Send + Sync>;

/// The real `recyclerClient` (`recycler_client.go:176-267`) over [`Storage`].
pub struct StorageRecyclerClient<S: Storage> {
    storage: Arc<S>,
    recorder: RecycleEventRecorder,
}

impl<S: Storage + 'static> StorageRecyclerClient<S> {
    /// `recorder` is the `RecycleEventRecorder`.
    pub fn new(storage: Arc<S>, recorder: RecycleEventRecorder) -> Self {
        Self { storage, recorder }
    }
}

#[async_trait]
impl<S: Storage + 'static> RecyclerClient for StorageRecyclerClient<S> {
    async fn create_pod(&self, pod: &Pod) -> Result<(), CreatePodError> {
        let ns = pod.metadata.namespace.as_deref().unwrap_or("default");
        let key = build_key("pods", Some(ns), &pod.metadata.name);
        let mut pod = pod.clone();
        if pod.metadata.uid.is_empty() {
            pod.metadata.uid = uuid::Uuid::new_v4().to_string();
        }
        if pod.metadata.creation_timestamp.is_none() {
            pod.metadata.creation_timestamp = Some(chrono::Utc::now());
        }
        match self.storage.create(&key, &pod).await {
            Ok(_) => Ok(()),
            Err(rusternetes_common::Error::AlreadyExists(_)) => Err(CreatePodError::AlreadyExists),
            Err(e) => Err(CreatePodError::Other(e.to_string())),
        }
    }

    async fn delete_pod(&self, name: &str, namespace: &str) -> Result<(), String> {
        let key = build_key("pods", Some(namespace), name);
        match self.storage.delete(&key).await {
            Ok(()) | Err(rusternetes_common::Error::NotFound(_)) => Ok(()),
            Err(e) => Err(e.to_string()),
        }
    }

    /// `WatchPod` (`recycler_client.go:199-267`): pod updates for exactly
    /// `name` plus every `Added` event whose involved object is named `name`.
    async fn watch_pod(
        &self,
        name: &str,
        namespace: &str,
    ) -> Result<BoxStream<'static, RecyclerWatchEvent>, String> {
        let pod_key = build_key("pods", Some(namespace), name);
        let pods = self
            .storage
            .watch(&pod_key)
            .await
            .map_err(|e| e.to_string())?;
        let events = self
            .storage
            .watch(&rusternetes_storage::build_prefix(
                "events",
                Some(namespace),
            ))
            .await
            .map_err(|e| e.to_string())?;
        let name = name.to_string();
        let pod_events = pods.filter_map(move |item| {
            let pod_key = pod_key.clone();
            async move {
                match item {
                    Ok(WatchEvent::Added(k, v)) if k == pod_key => serde_json::from_str(&v)
                        .ok()
                        .map(RecyclerWatchEvent::PodAdded),
                    Ok(WatchEvent::Modified(k, v)) if k == pod_key => serde_json::from_str(&v)
                        .ok()
                        .map(RecyclerWatchEvent::PodModified),
                    Ok(WatchEvent::Deleted(k, _)) if k == pod_key => {
                        Some(RecyclerWatchEvent::PodDeleted)
                    }
                    Err(_) => Some(RecyclerWatchEvent::PodError),
                    _ => None,
                }
            }
        });
        let involved = name;
        let event_events = events.filter_map(move |item| {
            let involved = involved.clone();
            async move {
                let Ok(WatchEvent::Added(_, v)) = item else {
                    return None;
                };
                let ev: Event = serde_json::from_str(&v).ok()?;
                if ev.involved_object.name.as_deref() != Some(involved.as_str()) {
                    return None;
                }
                Some(RecyclerWatchEvent::Event {
                    event_type: format!("{:?}", ev.event_type),
                    message: ev.message,
                })
            }
        });
        Ok(futures::stream::select(pod_events, event_events).boxed())
    }

    fn event(&self, event_type: &str, message: &str) {
        (self.recorder)(event_type, message);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    fn pv_with_capacity(capacity: &str) -> PersistentVolume {
        let mut pv: PersistentVolume = serde_json::from_value(serde_json::json!({
            "apiVersion": "v1", "kind": "PersistentVolume",
            "metadata": {"name": "pv"},
            "spec": {"capacity": {"storage": capacity}},
        }))
        .unwrap();
        pv.spec.host_path = Some(
            rusternetes_common::resources::volume::HostPathVolumeSource {
                path: "/tmp/x".to_string(),
                r#type: None,
            },
        );
        pv
    }

    /// `util.CalculateTimeoutForVolume`: max(minimum, increment * whole Gi).
    #[test]
    fn calculate_timeout_for_volume_is_max_of_minimum_and_per_gi_increment() {
        assert_eq!(
            calculate_timeout_for_volume(60, 30, &pv_with_capacity("1Gi")),
            60
        );
        assert_eq!(
            calculate_timeout_for_volume(60, 30, &pv_with_capacity("2Gi")),
            60
        );
        assert_eq!(
            calculate_timeout_for_volume(60, 30, &pv_with_capacity("3Gi")),
            90
        );
        assert_eq!(
            calculate_timeout_for_volume(300, 30, &pv_with_capacity("10Gi")),
            300
        );
        assert_eq!(
            calculate_timeout_for_volume(300, 30, &pv_with_capacity("20Gi")),
            600
        );
        // Sub-Gi sizes give 0 increments -> the minimum.
        assert_eq!(
            calculate_timeout_for_volume(60, 30, &pv_with_capacity("500Mi")),
            60
        );
        assert_eq!(
            calculate_timeout_for_volume(60, 30, &pv_with_capacity("100G")),
            93 * 30
        );
    }

    /// `NewPersistentVolumeRecyclerPodTemplate` + the hostPath plugin's
    /// overrides (`plugins.go:978-1014`, `host_path.go:161-168`).
    #[test]
    fn host_path_recycler_pod_matches_upstream_template() {
        let pv = pv_with_capacity("5Gi");
        let pod = host_path_recycler_pod(&pv).unwrap();
        assert_eq!(pod.metadata.generate_name.as_deref(), Some("pv-recycler-"));
        assert_eq!(pod.metadata.namespace.as_deref(), Some("default"));
        let spec = pod.spec.unwrap();
        assert_eq!(spec.active_deadline_seconds, Some(150));
        assert_eq!(spec.restart_policy.as_deref(), Some("Never"));
        assert_eq!(
            spec.volumes.as_ref().unwrap()[0]
                .host_path
                .as_ref()
                .unwrap()
                .path,
            "/tmp/x"
        );
        let c = &spec.containers[0];
        assert_eq!(c.name, "pv-recycler");
        assert_eq!(
            c.image,
            "registry.k8s.io/build-image/debian-base:bookworm-v1.0.6"
        );
        assert_eq!(c.command.as_deref(), Some(&["/bin/sh".to_string()][..]));
        assert_eq!(
            c.args.as_ref().unwrap()[1],
            "test -e /scrub && find /scrub -mindepth 1 -delete && test -z \"$(ls -A /scrub)\" || exit 1"
        );
        let m = &c.volume_mounts.as_ref().unwrap()[0];
        assert_eq!((m.name.as_str(), m.mount_path.as_str()), ("vol", "/scrub"));
    }

    /// `nfsPlugin.Recycle` (`nfs.go:161-171`): generateName, 300s floor, NFS
    /// source copied from the PV.
    #[test]
    fn nfs_recycler_pod_matches_upstream_plugin() {
        let mut pv = pv_with_capacity("1Gi");
        pv.spec.host_path = None;
        pv.spec.nfs = Some(rusternetes_common::resources::volume::NFSVolumeSource {
            server: "srv".to_string(),
            path: "/exp".to_string(),
            read_only: None,
        });
        let pod = nfs_recycler_pod(&pv).unwrap();
        assert_eq!(
            pod.metadata.generate_name.as_deref(),
            Some("pv-recycler-nfs-")
        );
        let spec = pod.spec.unwrap();
        assert_eq!(spec.active_deadline_seconds, Some(300));
        let nfs = spec.volumes.as_ref().unwrap()[0].nfs.as_ref().unwrap();
        assert_eq!((nfs.server.as_str(), nfs.path.as_str()), ("srv", "/exp"));
    }

    #[test]
    fn plugins_reject_a_pv_without_their_source() {
        let mut pv = pv_with_capacity("1Gi");
        pv.spec.host_path = None;
        assert_eq!(
            host_path_recycler_pod(&pv).unwrap_err(),
            "spec.PersistentVolume.Spec.HostPath is nil"
        );
        assert_eq!(
            nfs_recycler_pod(&pv).unwrap_err(),
            "spec.PersistentVolumeSource.NFS is nil"
        );
        assert!(recycler_pod_for_volume(&pv).is_none());
    }

    // ---- recycler_client_test.go TestRecyclerPod ----

    struct MockClient {
        existing: Mutex<bool>,
        events: Mutex<Vec<RecyclerWatchEvent>>,
        deleted: Mutex<bool>,
        received: Mutex<Vec<(String, String)>>,
    }

    #[async_trait]
    impl RecyclerClient for MockClient {
        async fn create_pod(&self, _pod: &Pod) -> Result<(), CreatePodError> {
            let mut existing = self.existing.lock().unwrap();
            if *existing {
                return Err(CreatePodError::AlreadyExists);
            }
            *existing = true;
            Ok(())
        }
        async fn delete_pod(&self, _n: &str, _ns: &str) -> Result<(), String> {
            *self.deleted.lock().unwrap() = true;
            Ok(())
        }
        async fn watch_pod(
            &self,
            _n: &str,
            _ns: &str,
        ) -> Result<BoxStream<'static, RecyclerWatchEvent>, String> {
            let evs = self.events.lock().unwrap().clone();
            // Like the mock channel, it is never closed by the sender side
            // before the consumer is done: the stream ends after the events.
            Ok(futures::stream::iter(evs).boxed())
        }
        fn event(&self, t: &str, m: &str) {
            self.received.lock().unwrap().push((t.into(), m.into()));
        }
    }

    fn pod_in(phase: Phase, message: &str) -> Pod {
        let mut pod = new_persistent_volume_recycler_pod_template();
        pod.status = Some(rusternetes_common::resources::pod::PodStatus {
            phase: Some(phase),
            message: if message.is_empty() {
                None
            } else {
                Some(message.to_string())
            },
            ..Default::default()
        });
        pod
    }

    fn ev(t: &str, m: &str) -> RecyclerWatchEvent {
        RecyclerWatchEvent::Event {
            event_type: t.into(),
            message: m.into(),
        }
    }

    async fn run(
        existing: bool,
        events: Vec<RecyclerWatchEvent>,
    ) -> (Result<(), String>, MockClient) {
        let client = MockClient {
            existing: Mutex::new(existing),
            events: Mutex::new(events),
            deleted: Mutex::new(false),
            received: Mutex::new(vec![]),
        };
        let r = recycle_volume_by_watching_pod_until_completion(
            "pv",
            new_persistent_volume_recycler_pod_template(),
            &client,
        )
        .await;
        (r, client)
    }

    /// "RecyclerSuccess": events are forwarded to the volume, pod Succeeded.
    #[tokio::test]
    async fn recycler_success_forwards_events_and_deletes_pod() {
        let (r, c) = run(
            false,
            vec![
                RecyclerWatchEvent::PodAdded(pod_in(Phase::Pending, "")),
                ev(
                    "Normal",
                    "Successfully assigned recycler-for-pv to 127.0.0.1",
                ),
                ev("Normal", "Pulling image"),
                RecyclerWatchEvent::PodModified(pod_in(Phase::Running, "")),
                RecyclerWatchEvent::PodModified(pod_in(Phase::Succeeded, "")),
            ],
        )
        .await;
        assert_eq!(r, Ok(()));
        assert!(*c.deleted.lock().unwrap(), "recycler pod must be deleted");
        let got = c.received.lock().unwrap().clone();
        assert_eq!(
            got,
            vec![
                (
                    "Normal".to_string(),
                    "Successfully assigned recycler-for-pv to 127.0.0.1".to_string()
                ),
                ("Normal".to_string(), "Pulling image".to_string()),
            ]
        );
    }

    /// "RecyclerFailure": pod Failed with a message.
    #[tokio::test]
    async fn recycler_failure_reports_pod_message() {
        let (r, c) = run(
            false,
            vec![
                RecyclerWatchEvent::PodAdded(pod_in(Phase::Pending, "")),
                ev("Warning", "Unable to mount volumes"),
                RecyclerWatchEvent::PodModified(pod_in(
                    Phase::Failed,
                    "Pod was active on the node longer than specified deadline",
                )),
            ],
        )
        .await;
        assert_eq!(
            r,
            Err("failed to recycle volume: Pod was active on the node longer than specified deadline".to_string())
        );
        assert!(*c.deleted.lock().unwrap());
        assert_eq!(c.received.lock().unwrap().len(), 1);
    }

    /// A Failed pod with no message.
    #[tokio::test]
    async fn recycler_failure_without_message() {
        let (r, _) = run(
            false,
            vec![RecyclerWatchEvent::PodModified(pod_in(Phase::Failed, ""))],
        )
        .await;
        assert_eq!(
            r,
            Err("failed to recycle volume: pod failed, pod.Status.Message unknown".to_string())
        );
    }

    /// "RecyclerDeleted": the recycler pod gets deleted under us.
    #[tokio::test]
    async fn recycler_pod_deleted_fails() {
        let (r, c) = run(
            false,
            vec![
                RecyclerWatchEvent::PodAdded(pod_in(Phase::Pending, "")),
                RecyclerWatchEvent::PodDeleted,
            ],
        )
        .await;
        assert_eq!(
            r,
            Err("failed to recycle volume: recycler pod was deleted".to_string())
        );
        assert!(*c.deleted.lock().unwrap());
    }

    /// "RecyclerRunning": another recycler pod with the name already exists;
    /// it is deleted and the recycle retried later.
    #[tokio::test]
    async fn existing_recycler_pod_is_deleted_and_retried_later() {
        let (r, c) = run(true, vec![]).await;
        assert_eq!(
            r,
            Err("old recycler pod found, will retry later".to_string())
        );
        assert!(*c.deleted.lock().unwrap());
    }

    /// `waitForPod`: a closed watch channel is an error.
    #[tokio::test]
    async fn closed_watch_channel_fails() {
        let (r, _) = run(false, vec![]).await;
        assert_eq!(
            r,
            Err("failed to recycle volume: recycler pod \"recycler-for-pv\" watch channel had been closed".to_string())
        );
    }

    /// The recycler pod is renamed `recycler-for-<pv>` and loses generateName
    /// (`recycler_client.go:62-63`).
    #[tokio::test]
    async fn storage_client_creates_pod_named_for_pv() {
        use rusternetes_storage::MemoryStorage;
        let storage = Arc::new(MemoryStorage::new());
        let client = StorageRecyclerClient::new(storage.clone(), Box::new(|_, _| {}));
        // Succeed straight away: a kubelet stand-in marks the pod Succeeded.
        let s2 = storage.clone();
        let kubelet = tokio::spawn(async move {
            let key = build_key("pods", Some("default"), "recycler-for-pv");
            loop {
                if let Ok(mut p) = s2.get::<Pod>(&key).await {
                    assert!(p.metadata.generate_name.is_none());
                    p.status = Some(rusternetes_common::resources::pod::PodStatus {
                        phase: Some(Phase::Succeeded),
                        ..Default::default()
                    });
                    s2.update(&key, &p).await.unwrap();
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        });
        let r = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            recycle_volume_by_watching_pod_until_completion(
                "pv",
                new_persistent_volume_recycler_pod_template(),
                &client,
            ),
        )
        .await
        .expect("recycle must not hang");
        kubelet.await.unwrap();
        assert_eq!(r, Ok(()));
        assert!(storage
            .get::<Pod>(&build_key("pods", Some("default"), "recycler-for-pv"))
            .await
            .is_err());
    }
}
