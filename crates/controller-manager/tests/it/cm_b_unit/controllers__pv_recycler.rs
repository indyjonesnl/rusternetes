//! Unit tests moved from `src/controllers/pv_recycler.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use async_trait::async_trait;
    pub use futures::stream::BoxStream;
    pub use futures::StreamExt;
    pub use rusternetes_common::resources::{Event, PersistentVolume, Pod};
    pub use rusternetes_common::types::Phase;
    pub use rusternetes_storage::{build_key, Storage, WatchEvent};
    pub use std::sync::Arc;
}

mod tests {
    use super::__parent::*;
    use rusternetes_controller_manager::controllers::pv_recycler::*;
    use std::sync::Mutex;

    fn json(p: &Pod) -> serde_json::Value {
        serde_json::to_value(p).unwrap()
    }

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
        let pod = host_path_recycler_pod(&pv, &VolumeConfig::host_path_default()).unwrap();
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
        let pod = nfs_recycler_pod(&pv, &VolumeConfig::nfs_default()).unwrap();
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
            host_path_recycler_pod(&pv, &VolumeConfig::host_path_default()).unwrap_err(),
            "spec.PersistentVolume.Spec.HostPath is nil"
        );
        assert_eq!(
            nfs_recycler_pod(&pv, &VolumeConfig::nfs_default()).unwrap_err(),
            "spec.PersistentVolumeSource.NFS is nil"
        );
        assert!(recycler_pod_for_volume(&pv, &RecyclerConfig::default()).is_none());
    }

    // ---- pkg/volume/plugins_test.go Test_ValidatePodTemplate ----

    #[test]
    fn validate_recycler_pod_template_requires_a_volume() {
        let pod: Pod = serde_json::from_value(serde_json::json!({
            "apiVersion": "v1", "kind": "Pod", "metadata": {"name": "p"},
            "spec": {"volumes": [{"name": "vol"}], "containers": []},
        }))
        .unwrap();
        assert_eq!(validate_recycler_pod_template(&pod), Ok(()));
        // The default recycle pod template is valid.
        assert_eq!(
            validate_recycler_pod_template(&new_persistent_volume_recycler_pod_template()),
            Ok(())
        );
        let pod: Pod = serde_json::from_value(serde_json::json!({
            "apiVersion": "v1", "kind": "Pod", "metadata": {"name": "p"},
            "spec": {"containers": [{"name": "pv-recycler", "image": "x"}]},
        }))
        .unwrap();
        assert_eq!(
            validate_recycler_pod_template(&pod).unwrap_err(),
            "does not contain any volume(s)"
        );
    }

    // ---- cmd/kube-controller-manager/app/plugins.go AttemptToLoadRecycler ----

    const CUSTOM_TEMPLATE: &str = r#"
apiVersion: v1
kind: Pod
metadata:
  generateName: custom-recycler-
spec:
  restartPolicy: Never
  volumes:
  - name: vol
  containers:
  - name: scrub
    image: example/scrub:1
"#;

    #[test]
    fn attempt_to_load_recycler_empty_path_keeps_default_template() {
        let mut cfg = VolumeConfig::nfs_default();
        attempt_to_load_recycler("", &mut cfg).unwrap();
        assert_eq!(
            json(&cfg.recycler_pod_template),
            json(&new_persistent_volume_recycler_pod_template())
        );
    }

    #[test]
    fn attempt_to_load_recycler_loads_and_validates_the_template() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pod.yaml");
        std::fs::write(&path, CUSTOM_TEMPLATE).unwrap();
        let mut cfg = VolumeConfig::nfs_default();
        attempt_to_load_recycler(path.to_str().unwrap(), &mut cfg).unwrap();
        assert_eq!(
            cfg.recycler_pod_template.metadata.generate_name.as_deref(),
            Some("custom-recycler-")
        );

        // A template without volumes: "pod specification (<path>): ..."
        let bad = dir.path().join("bad.yaml");
        std::fs::write(
            &bad,
            "apiVersion: v1\nkind: Pod\nmetadata:\n  name: p\nspec:\n  containers:\n  - name: c\n    image: i\n",
        )
        .unwrap();
        let mut cfg = VolumeConfig::nfs_default();
        let err = attempt_to_load_recycler(bad.to_str().unwrap(), &mut cfg).unwrap_err();
        assert_eq!(
            err,
            format!(
                "pod specification ({}): does not contain any volume(s)",
                bad.display()
            )
        );
        // A failed load leaves the config untouched.
        assert_eq!(
            json(&cfg.recycler_pod_template),
            json(&new_persistent_volume_recycler_pod_template())
        );
    }

    /// `LoadPodFromFile` (`pkg/volume/util/util.go:128-146`) errors.
    #[test]
    fn load_pod_from_file_errors() {
        assert_eq!(
            load_pod_from_file("").unwrap_err(),
            "file path not specified"
        );
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("nope.yaml");
        let err = load_pod_from_file(missing.to_str().unwrap()).unwrap_err();
        assert!(
            err.starts_with(&format!("failed to read file path {}: ", missing.display())),
            "{err}"
        );
        let empty = dir.path().join("empty.yaml");
        std::fs::write(&empty, "").unwrap();
        assert_eq!(
            load_pod_from_file(empty.to_str().unwrap()).unwrap_err(),
            format!("file was empty: {}", empty.display())
        );
        let junk = dir.path().join("junk.yaml");
        std::fs::write(&junk, "spec: [").unwrap();
        assert!(load_pod_from_file(junk.to_str().unwrap())
            .unwrap_err()
            .starts_with("failed decoding file: "));
    }

    // ---- ProbeProvisionableRecyclableVolumePlugins config (plugins.go:100-120) ----

    #[test]
    fn recycler_config_defaults_match_upstream_flag_defaults() {
        let c = PersistentVolumeRecyclerConfiguration::default();
        assert_eq!(
            (
                c.minimum_timeout_nfs,
                c.increment_timeout_nfs,
                c.minimum_timeout_host_path,
                c.increment_timeout_host_path
            ),
            (300, 30, 60, 30)
        );
        let r = RecyclerConfig::from_configuration(&c).unwrap();
        let d = RecyclerConfig::default();
        assert_eq!(
            r.nfs.recycler_minimum_timeout,
            d.nfs.recycler_minimum_timeout
        );
        assert_eq!(
            json(&r.host_path.recycler_pod_template),
            json(&d.host_path.recycler_pod_template)
        );
    }

    #[test]
    fn recycler_config_flows_timeouts_and_templates_into_pods() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pod.yaml");
        std::fs::write(&path, CUSTOM_TEMPLATE).unwrap();
        let c = PersistentVolumeRecyclerConfiguration {
            minimum_timeout_nfs: 1000,
            increment_timeout_nfs: 10,
            pod_template_file_path_nfs: path.to_str().unwrap().to_string(),
            minimum_timeout_host_path: 90,
            increment_timeout_host_path: 5,
            pod_template_file_path_host_path: String::new(),
        };
        let r = RecyclerConfig::from_configuration(&c).unwrap();
        let mut pv = pv_with_capacity("1Gi");
        let pod = host_path_recycler_pod(&pv, &r.host_path).unwrap();
        assert_eq!(pod.spec.unwrap().active_deadline_seconds, Some(90));
        pv.spec.host_path = None;
        pv.spec.nfs = Some(rusternetes_common::resources::volume::NFSVolumeSource {
            server: "s".to_string(),
            path: "/e".to_string(),
            read_only: None,
        });
        let pod = nfs_recycler_pod(&pv, &r.nfs).unwrap();
        // nfs.go:164 forces the generateName even over a loaded template.
        assert_eq!(
            pod.metadata.generate_name.as_deref(),
            Some("pv-recycler-nfs-")
        );
        let spec = pod.spec.unwrap();
        assert_eq!(spec.active_deadline_seconds, Some(1000));
        assert_eq!(spec.containers[0].name, "scrub");
    }

    #[test]
    fn recycler_config_fails_on_a_bad_template_path() {
        let c = PersistentVolumeRecyclerConfiguration {
            pod_template_file_path_host_path: "/nonexistent/pod.yaml".to_string(),
            ..Default::default()
        };
        assert!(RecyclerConfig::from_configuration(&c)
            .unwrap_err()
            .starts_with("failed to read file path /nonexistent/pod.yaml: "));
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
                    s2.update_status(&key, &p).await.unwrap();
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
