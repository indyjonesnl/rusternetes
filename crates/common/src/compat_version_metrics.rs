//! Port of `k8s.io/component-base/metrics/prometheus/compatversion`
//! (`metrics.go:27-50`): the `version_info` gauge, registered in the process
//! default registry (this port's `legacyregistry`, served on `/metrics`).

use crate::compatibility::EffectiveVersion;
use prometheus::{GaugeVec, Opts};
use std::sync::LazyLock;

/// `compatVersionInfo` (metrics.go:28-37): no namespace/subsystem, ALPHA
/// stability (legacyregistry renders it as a `[ALPHA] ` help prefix). Registered
/// in the default registry on first use, as `init()` does (metrics.go:48-50).
static COMPAT_VERSION_INFO: LazyLock<GaugeVec> = LazyLock::new(|| {
    let g = GaugeVec::new(
        Opts::new(
            "version_info",
            "[ALPHA] Provides the compatibility version info of the component. \
             The component label is the name of the component, usually kube, \
             but is relevant for aggregated-apiservers.",
        ),
        &["component", "binary", "emulation", "min_compat"],
    )
    .expect("valid version_info opts");
    prometheus::default_registry()
        .register(Box::new(g.clone()))
        .expect("version_info registered once");
    g
});

/// `ResetCompatVersionInfoMetric` (metrics.go:40-42).
pub fn reset_compat_version_info_metric() {
    COMPAT_VERSION_INFO.reset();
}

/// `RecordCompatVersionInfo` (metrics.go:44-46).
pub fn record_compat_version_info(
    component: &str,
    binary: &str,
    emulation: &str,
    min_compat: &str,
) {
    COMPAT_VERSION_INFO
        .with_label_values(&[component, binary, emulation, min_compat])
        .set(1.0);
}

/// `componentGlobalsRegistry.AddMetrics` (compatibility/registry.go:123-131)
/// for one registered component's effective version.
pub fn add_metrics(component: &str, effective_version: &EffectiveVersion) {
    record_compat_version_info(
        component,
        &effective_version.binary_version().to_string(),
        &effective_version.emulation_version().to_string(),
        &effective_version.min_compatibility_version().to_string(),
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    fn gather_version_info() -> String {
        use prometheus::Encoder;
        let fams: Vec<_> = prometheus::default_registry()
            .gather()
            .into_iter()
            .filter(|f| f.name() == "version_info")
            .collect();
        let mut buf = Vec::new();
        prometheus::TextEncoder::new()
            .encode(&fams, &mut buf)
            .unwrap();
        String::from_utf8(buf).unwrap()
    }

    // metrics_test.go TestRecordCompatVersionInfo
    #[test]
    #[serial]
    fn record_compat_version_info_exposition() {
        reset_compat_version_info_metric();
        record_compat_version_info("name", "1.28", "1.27", "1.26");
        let want = "# HELP version_info [ALPHA] Provides the compatibility version info of the component. The component label is the name of the component, usually kube, but is relevant for aggregated-apiservers.\n\
# TYPE version_info gauge\n\
version_info{binary=\"1.28\",component=\"name\",emulation=\"1.27\",min_compat=\"1.26\"} 1\n";
        assert_eq!(gather_version_info(), want);
    }

    // registry.go AddMetrics: records binary/emulation/minCompat of the component.
    #[test]
    #[serial]
    fn add_metrics_records_effective_version() {
        reset_compat_version_info_metric();
        let ev = crate::compatibility::default_build_effective_version();
        add_metrics("kube", &ev);
        let got = gather_version_info();
        assert!(
            got.contains(
                "version_info{binary=\"1.35\",component=\"kube\",emulation=\"1.35\",min_compat=\"1.34\"} 1\n"
            ),
            "{got}"
        );
    }
}
