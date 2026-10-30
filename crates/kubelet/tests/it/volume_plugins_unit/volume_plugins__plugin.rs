//! Unit tests moved from `src/volume_plugins/plugin.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::Result;
    pub use async_trait::async_trait;
    pub use rusternetes_common::quantity::Quantity;
    pub use rusternetes_common::resources::{PersistentVolume, Pod, Volume};
}

mod block_tests {
    use super::__parent::*;
    use rusternetes_kubelet::volume_plugins::plugin::*;

    struct Plain;
    impl MetricsProvider for Plain {
        fn get_metrics(&self) -> Result<Metrics> {
            Ok(Metrics::default())
        }
    }
    impl BlockVolumeMapper for Plain {
        fn get_global_map_path(&self, _s: &Spec<'_>) -> Result<String> {
            Ok("g".into())
        }
        fn get_pod_device_map_path(&self) -> (String, String) {
            ("p".into(), "n".into())
        }
        fn supports_metrics(&self) -> bool {
            false
        }
    }

    struct Custom;
    impl MetricsProvider for Custom {
        fn get_metrics(&self) -> Result<Metrics> {
            Ok(Metrics::default())
        }
    }
    impl BlockVolumeMapper for Custom {
        fn get_global_map_path(&self, _s: &Spec<'_>) -> Result<String> {
            Ok("g".into())
        }
        fn get_pod_device_map_path(&self) -> (String, String) {
            ("p".into(), "n".into())
        }
        fn supports_metrics(&self) -> bool {
            true
        }
        fn as_custom_block_volume_mapper(&self) -> Option<&dyn CustomBlockVolumeMapper> {
            Some(self)
        }
    }
    #[async_trait]
    impl CustomBlockVolumeMapper for Custom {
        async fn set_up_device(&self) -> Result<String> {
            Ok("/staging".into())
        }
        async fn map_pod_device(&self) -> Result<String> {
            Ok(String::new())
        }
        fn get_staging_path(&self) -> String {
            "/staging".into()
        }
    }

    /// The operation generator type-asserts a mapper to
    /// `CustomBlockVolumeMapper`; a plain mapper must fail that assertion.
    #[tokio::test]
    async fn custom_mapper_assertion() {
        assert!(Plain.as_custom_block_volume_mapper().is_none());
        let c = Custom;
        let custom = c.as_custom_block_volume_mapper().unwrap();
        assert_eq!(custom.set_up_device().await.unwrap(), "/staging");
        assert_eq!(custom.map_pod_device().await.unwrap(), "");
        assert_eq!(custom.get_staging_path(), "/staging");
        assert!(c.supports_metrics());
    }
}
