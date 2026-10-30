//! Unit tests moved from `src/volume_plugins/util/exponential_backoff.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use std::time::{Duration, Instant};
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubelet::volume_plugins::util::exponential_backoff::*;

    #[test]
    fn doubles_from_500ms_and_caps() {
        let mut b = ExponentialBackoff::default();
        assert!(b.safe_to_retry("x").is_ok());
        b.update("e");
        assert_eq!(b.duration_before_retry(), Duration::from_millis(500));
        assert!(b.safe_to_retry("x").is_err());
        b.update("e");
        assert_eq!(b.duration_before_retry(), Duration::from_secs(1));
        for _ in 0..20 {
            b.update("e");
        }
        assert_eq!(b.duration_before_retry(), MAX_DURATION_BEFORE_RETRY);
    }
}
