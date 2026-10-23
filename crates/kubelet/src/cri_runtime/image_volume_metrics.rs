//! Port of the image-volume kubelet metrics: `ImageVolumeRequestedTotal`,
//! `ImageVolumeMountedSucceedTotal` and `ImageVolumeMountedErrorsTotal`
//! (`pkg/kubelet/metrics/metrics.go:165-167`, `:1101-1129`, registered under
//! the `ImageVolume` gate at `:1300-1304`) and `incrementImageVolumeMetrics`
//! (`pkg/kubelet/kuberuntime/kuberuntime_manager.go:1745-1763`).

use prometheus::{IntCounter, Opts};
use rusternetes_common::feature_gates::{enabled, Feature};
use rusternetes_common::resources::pod::Container;
use std::collections::HashMap;
use std::sync::{LazyLock, Once};

/// `crierror.ErrImageVolumeMountFailed.Error()`
/// (`staging/src/k8s.io/cri-api/pkg/errors/errors.go:37`).
pub const ERR_IMAGE_VOLUME_MOUNT_FAILED: &str = "ImageVolumeMountFailed";

fn counter(name: &str, help: &str) -> IntCounter {
    IntCounter::with_opts(Opts::new(name, help).namespace("kubelet")).expect("valid counter")
}

/// `ImageVolumeRequestedTotal` (`metrics.go:1101-1109`).
pub static IMAGE_VOLUME_REQUESTED_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    counter(
        "image_volume_requested_total",
        "[BETA] Number of requested image volumes.",
    )
});
/// `ImageVolumeMountedSucceedTotal` (`metrics.go:1111-1119`).
pub static IMAGE_VOLUME_MOUNTED_SUCCEED_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    counter(
        "image_volume_mounted_succeed_total",
        "[BETA] Number of successful image volume mounts.",
    )
});
/// `ImageVolumeMountedErrorsTotal` (`metrics.go:1121-1129`).
pub static IMAGE_VOLUME_MOUNTED_ERRORS_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    counter(
        "image_volume_mounted_errors_total",
        "[BETA] Number of failed image volume mounts.",
    )
});

static REGISTER: Once = Once::new();

/// `legacyregistry.MustRegister` of the three counters, only when the
/// `ImageVolume` gate is on (`metrics.go:1300-1304`).
pub fn register_image_volume_metrics() {
    if !enabled(Feature::ImageVolume) {
        return;
    }
    REGISTER.call_once(|| {
        let registry = prometheus::default_registry();
        for c in [
            &IMAGE_VOLUME_REQUESTED_TOTAL,
            &IMAGE_VOLUME_MOUNTED_SUCCEED_TOTAL,
            &IMAGE_VOLUME_MOUNTED_ERRORS_TOTAL,
        ] {
            let _ = registry.register(Box::new((**c).clone()));
        }
    });
}

/// Port of `incrementImageVolumeMetrics` (`kuberuntime_manager.go:1748-1763`).
/// `is_create_container_err` is `errors.Is(err, ErrCreateContainer)`; any
/// other outcome, success included, counts as a succeeded mount, exactly as
/// upstream.
pub fn increment_image_volume_metrics<V>(
    is_create_container_err: bool,
    msg: &str,
    container: &Container,
    image_volumes: &HashMap<String, V>,
) {
    if !enabled(Feature::ImageVolume) {
        return;
    }
    IMAGE_VOLUME_REQUESTED_TOTAL.inc_by(image_volumes.len() as u64);
    for m in container.volume_mounts.iter().flatten() {
        if image_volumes.contains_key(&m.name) {
            if is_create_container_err && msg.starts_with(ERR_IMAGE_VOLUME_MOUNT_FAILED) {
                IMAGE_VOLUME_MOUNTED_ERRORS_TOTAL.inc();
            } else {
                IMAGE_VOLUME_MOUNTED_SUCCEED_TOTAL.inc();
            }
        }
    }
}

/// `grpcstatus.FromError(err)` then `.Message()`: the gRPC status message for
/// a CRI error, the error text otherwise.
pub fn grpc_message(err: &anyhow::Error) -> String {
    for cause in err.chain() {
        if let Some(rusternetes_cri::CriError::Rpc { source, .. }) =
            cause.downcast_ref::<rusternetes_cri::CriError>()
        {
            return source.message().to_string();
        }
    }
    format!("{err:#}")
}

/// The `Error: %v` message of every `FailedToCreateContainer` event
/// `toKubeContainerImageVolumes` records (`kuberuntime_manager.go:1792-1800`),
/// one per failed pull of a volume the container mounts, in mount order.
pub fn image_volume_pull_failure_events<V>(
    pulls: &HashMap<String, Result<V, std::sync::Arc<anyhow::Error>>>,
    container: &Container,
) -> Vec<String> {
    container
        .volume_mounts
        .iter()
        .flatten()
        .filter_map(|m| match pulls.get(&m.name) {
            Some(Err(e)) => Some(format!("Error: {}", grpc_message(e))),
            _ => None,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::feature_gates::{with_feature, Feature};
    use rusternetes_common::resources::pod::VolumeMount;
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
