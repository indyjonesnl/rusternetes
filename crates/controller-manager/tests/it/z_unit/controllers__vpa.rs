//! Unit tests moved from `src/controllers/vpa.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::{Context, Result};
    pub use rusternetes_common::quantity::parse_resource_value;
    pub use rusternetes_common::resources::{
        Deployment, Pod, RecommendedContainerResources, RecommendedPodResources, ReplicaSet,
        StatefulSet, VerticalPodAutoscaler, VerticalPodAutoscalerStatus,
    };
    pub use rusternetes_storage::{build_key, extract_key, Storage, WorkQueue};
    pub use std::collections::HashMap;
    pub use std::sync::Arc;
    pub use tokio::time::Duration;
    pub use tracing::{debug, error, info, warn};
}

mod tests {
    use super::__parent::*;
    use rusternetes_controller_manager::controllers::vpa::*;

    #[test]
    fn test_parse_cpu_string() {
        assert_eq!(parse_cpu_string("100m"), Some(100));
        assert_eq!(parse_cpu_string("1"), Some(1000));
        assert_eq!(parse_cpu_string("2"), Some(2000));
        assert_eq!(parse_cpu_string("500m"), Some(500));
    }

    #[test]
    fn test_parse_memory_string() {
        assert_eq!(parse_memory_string("128Mi"), Some(128 * 1024 * 1024));
        assert_eq!(parse_memory_string("1Gi"), Some(1024 * 1024 * 1024));
        assert_eq!(parse_memory_string("512Ki"), Some(512 * 1024));
        assert_eq!(parse_memory_string("1024"), Some(1024));
    }

    /// `minAllowed`/`maxAllowed` and container requests carry the full
    /// quantity grammar. A decimal point used to yield `None`, which drops the
    /// bound and lets the recommendation escape the policy.
    #[test]
    fn test_parse_quantity_strings_cover_full_grammar() {
        assert_eq!(parse_cpu_string("0.5"), Some(500));
        assert_eq!(parse_cpu_string("1.5"), Some(1500));
        assert_eq!(parse_cpu_string("10.5m"), Some(11));
        assert_eq!(parse_memory_string("0.5Gi"), Some(536_870_912));
        assert_eq!(parse_memory_string("1.5Gi"), Some(1_610_612_736));
        // Decimal SI and the large binary suffixes were absent entirely.
        assert_eq!(parse_memory_string("1M"), Some(1_000_000));
        assert_eq!(parse_memory_string("1G"), Some(1_000_000_000));
        assert_eq!(parse_memory_string("1Pi"), Some(1_125_899_906_842_624));
        assert_eq!(parse_memory_string("1Ei"), Some(1_152_921_504_606_846_976));
    }

    /// `trim_end_matches` strips *every* trailing occurrence of the suffix, so
    /// `"1KiKi"` parsed as 1Ki. `strip_suffix` semantics reject it.
    #[test]
    fn test_parse_memory_string_rejects_repeated_suffix() {
        assert_eq!(parse_memory_string("1KiKi"), None);
        assert_eq!(parse_memory_string("1MiMi"), None);
        assert_eq!(parse_cpu_string("100mm"), None);
    }

    #[test]
    fn test_format_cpu() {
        assert_eq!(format_cpu(100), "100m");
        assert_eq!(format_cpu(1000), "1");
        assert_eq!(format_cpu(2000), "2");
        assert_eq!(format_cpu(1500), "1500m");
    }

    #[test]
    fn test_format_memory() {
        assert_eq!(format_memory(128 * 1024 * 1024), "128Mi");
        assert_eq!(format_memory(1024 * 1024 * 1024), "1Gi");
        assert_eq!(format_memory(512 * 1024), "512Ki");
        assert_eq!(format_memory(1024), "1Ki"); // 1024 bytes = 1 KiB
    }

    #[test]
    fn test_percentile() {
        let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        // 50th percentile: index = (0.5 * 9).round() = 4.5.round() = 5, value at index 5 is 6
        assert_eq!(percentile(&data, 50), 6); // median (rounded index)
        assert_eq!(percentile(&data, 95), 10); // 95th percentile
        assert_eq!(percentile(&data, 0), 1); // min
        assert_eq!(percentile(&data, 100), 10); // max
    }
}
