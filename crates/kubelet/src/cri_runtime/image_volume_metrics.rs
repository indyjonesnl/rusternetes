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
