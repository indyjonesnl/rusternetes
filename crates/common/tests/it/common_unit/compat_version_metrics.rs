//! Unit tests moved from `src/compat_version_metrics.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use prometheus::{GaugeVec, Opts};
    pub use rusternetes_common::compatibility::EffectiveVersion;
    pub use std::sync::LazyLock;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::compat_version_metrics::*;
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
        let ev = rusternetes_common::compatibility::default_build_effective_version();
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
