//! Unit tests moved from `src/tracing.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::Result;
    pub use std::sync::OnceLock;
    pub use tracing_subscriber::layer::SubscriberExt;
    pub use tracing_subscriber::util::SubscriberInitExt;
    pub use tracing_subscriber::{reload, EnvFilter, Registry};
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::tracing::*;

    #[test]
    fn verbosity_maps_to_levels() {
        assert_eq!(verbosity_directive(0, "warn,foo=debug"), "warn,foo=debug");
        assert_eq!(verbosity_directive(2, "warn"), "info");
        assert_eq!(verbosity_directive(4, "warn"), "debug");
        assert_eq!(verbosity_directive(5, "warn"), "debug");
        assert_eq!(verbosity_directive(6, "warn"), "trace");
        assert_eq!(verbosity_directive(10, "warn"), "trace");
    }
}
