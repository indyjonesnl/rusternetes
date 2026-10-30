//! Unit tests moved from `src/cri_runtime/image_volume_metrics.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use prometheus::{IntCounter, Opts};
    pub use rusternetes_common::feature_gates::{enabled, Feature};
    pub use rusternetes_common::resources::pod::Container;
    pub use std::collections::HashMap;
    pub use std::sync::{LazyLock, Once};
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::feature_gates::{with_feature, Feature};
    use rusternetes_common::resources::pod::VolumeMount;
    use rusternetes_kubelet::cri_runtime::image_volume_metrics::*;
    use std::sync::Arc;

    static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn mounts(names: &[&str]) -> Container {
        Container {
            volume_mounts: Some(
                names
                    .iter()
                    .map(|n| VolumeMount {
                        name: n.to_string(),
                        mount_path: format!("/{n}"),
                        read_only: None,
                        sub_path: None,
                        sub_path_expr: None,
                        mount_propagation: None,
                        recursive_read_only: None,
                    })
                    .collect(),
            ),
            ..Default::default()
        }
    }

    fn vols(names: &[&str]) -> HashMap<String, ()> {
        names.iter().map(|n| (n.to_string(), ())).collect()
    }

    fn counts() -> (u64, u64, u64) {
        (
            IMAGE_VOLUME_REQUESTED_TOTAL.get(),
            IMAGE_VOLUME_MOUNTED_SUCCEED_TOTAL.get(),
            IMAGE_VOLUME_MOUNTED_ERRORS_TOTAL.get(),
        )
    }

    fn reset() {
        IMAGE_VOLUME_REQUESTED_TOTAL.reset();
        IMAGE_VOLUME_MOUNTED_SUCCEED_TOTAL.reset();
        IMAGE_VOLUME_MOUNTED_ERRORS_TOTAL.reset();
    }

    type Case = (
        &'static str,
        bool,
        &'static str,
        Container,
        HashMap<String, ()>,
        (u64, u64, u64),
    );

    /// Port of `TestIncrementImageVolumeMetrics`
    /// (`kuberuntime_manager_test.go:4350-4475`); wants are (requested,
    /// succeed, errors).
    #[test]
    fn increment_image_volume_metrics_cases() {
        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let _gate = with_feature(Feature::ImageVolume, true);
        let three = vols(&["mount1", "mount2", "mount3"]);
        let two = || mounts(&["mount1", "mount2"]);
        let cases: Vec<Case> = vec![
            ("without error", false, "", two(), three.clone(), (3, 2, 0)),
            (
                "with error",
                true,
                ERR_IMAGE_VOLUME_MOUNT_FAILED,
                two(),
                three.clone(),
                (3, 0, 2),
            ),
            (
                "with unknown CRI error from message",
                true,
                "",
                two(),
                three.clone(),
                (3, 2, 0),
            ),
            (
                "without used volume",
                false,
                "",
                mounts(&["mount1"]),
                HashMap::new(),
                (0, 0, 0),
            ),
        ];
        for (name, is_create_err, msg, container, volumes, wants) in cases {
            reset();
            increment_image_volume_metrics(is_create_err, msg, &container, &volumes);
            assert_eq!(counts(), wants, "{name}");
        }
    }

    /// `incrementImageVolumeMetrics` returns first when the gate is off
    /// (`kuberuntime_manager.go:1749-1751`).
    #[test]
    fn increment_is_a_no_op_with_the_gate_off() {
        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let _gate = with_feature(Feature::ImageVolume, false);
        reset();
        increment_image_volume_metrics(false, "", &mounts(&["mount1"]), &vols(&["mount1"]));
        assert_eq!(counts(), (0, 0, 0));
    }

    /// The counters are served on `/metrics` under upstream's names
    /// (`kubelet` subsystem + key) once registered.
    #[test]
    fn counters_are_registered_with_upstream_names() {
        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let _gate = with_feature(Feature::ImageVolume, true);
        register_image_volume_metrics();
        register_image_volume_metrics();
        let names: Vec<String> = prometheus::default_registry()
            .gather()
            .iter()
            .map(|f| f.name().to_string())
            .collect();
        for n in [
            "kubelet_image_volume_requested_total",
            "kubelet_image_volume_mounted_succeed_total",
            "kubelet_image_volume_mounted_errors_total",
        ] {
            assert!(names.iter().any(|x| x == n), "{n} missing from {names:?}");
        }
    }

    /// `toKubeContainerImageVolumes` records one `FailedToCreateContainer`
    /// event per failed volume the container mounts, `Error: <message>`;
    /// volumes it does not mount record nothing.
    #[test]
    fn failed_pulls_record_one_event_per_mounted_volume() {
        let err = |m: &str| Err(Arc::new(anyhow::anyhow!(m.to_string())));
        let mut pulls: HashMap<String, Result<(), Arc<anyhow::Error>>> = HashMap::new();
        pulls.insert("a".into(), err("pull a failed"));
        pulls.insert("b".into(), Ok(()));
        pulls.insert("c".into(), err("pull c failed"));
        pulls.insert("unmounted".into(), err("x"));
        assert_eq!(
            image_volume_pull_failure_events(&pulls, &mounts(&["a", "b", "c"])),
            vec!["Error: pull a failed", "Error: pull c failed"]
        );
        assert!(image_volume_pull_failure_events(&pulls, &mounts(&["b"])).is_empty());
    }
}
