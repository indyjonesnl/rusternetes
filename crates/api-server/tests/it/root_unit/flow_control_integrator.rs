//! Unit tests moved from `src/flow_control_integrator.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_api_server::flow_control_queueset::Clock;
    pub use std::sync::{Arc, Mutex};
    pub use std::time::Duration;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::flow_control_integrator::test_clock::ManualClock;
    use rusternetes_api_server::flow_control_integrator::*;

    /// `TestIntegrator` (fairqueuing/integrator_test.go:27).
    #[test]
    fn integrator_matches_upstream() {
        let clk = ManualClock::new();
        let igr = Integrator::new(clk.clone());
        igr.add(3.0);
        clk.step(Duration::from_secs(1));
        let results = igr.get_results();
        let r_too = igr.reset();
        let e = IntegratorResults {
            duration: 1.0,
            average: 3.0,
            deviation: 0.0,
            min: 0.0,
            max: 3.0,
        };
        assert!(e.equal(&results), "expected {e:?}, got {results:?}");
        assert!(results.equal(&r_too), "expected {results:?}, got {r_too:?}");
        igr.set(2.0);
        let results = igr.get_results();
        let e = IntegratorResults {
            duration: 0.0,
            average: f64::NAN,
            deviation: f64::NAN,
            min: 2.0,
            max: 3.0,
        };
        assert!(e.equal(&results), "expected {e:?}, got {results:?}");
        clk.step(Duration::from_millis(1));
        igr.add(-1.0);
        clk.step(Duration::from_millis(1));
        let results = igr.get_results();
        let e = IntegratorResults {
            duration: 2.0 * Duration::from_millis(1).as_secs_f64(),
            average: 1.5,
            deviation: 0.5,
            min: 1.0,
            max: 3.0,
        };
        assert!(e.equal(&results), "expected {e:?}, got {results:?}");
    }
}
