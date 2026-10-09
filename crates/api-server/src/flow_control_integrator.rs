//! Port of `fairqueuing/integrator.go` (release-1.35): computes the moments
//! of a variable over time as read from a clock. The APF queueset feeds its
//! seat demand (`totSeatsInUse + totSeatsWaiting`) into one of these, and the
//! borrowing adjustment (`apf_controller.go` `updateBorrowingLocked`) resets
//! it every `borrowingAdjustmentPeriod` to read the demand statistics.

use crate::flow_control_queueset::Clock;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// `IntegratorResults` (integrator.go:44): statistical abstracts of the
/// integration.
#[derive(Clone, Copy, Debug)]
pub struct IntegratorResults {
    /// Seconds.
    pub duration: f64,
    /// Time-weighted.
    pub average: f64,
    /// `sqrt(avg((value-avg)^2))`.
    pub deviation: f64,
    pub min: f64,
    pub max: f64,
}

impl IntegratorResults {
    /// `Equal` (integrator.go:53): all NaNs are equal to each other.
    pub fn equal(&self, other: &IntegratorResults) -> bool {
        let f = |a: f64, b: f64| a == b || (a.is_nan() && b.is_nan());
        self.duration == other.duration
            && self.min == other.min
            && self.max == other.max
            && f(self.average, other.average)
            && f(self.deviation, other.deviation)
    }
}

/// `Moments` (integrator.go:160): the integrals of the 0, 1 and 2 powers of
/// some variable X over some range of time.
#[derive(Clone, Copy, Debug, Default)]
pub struct Moments {
    pub elapsed_seconds: f64,
    pub integral_x: f64,
    pub integral_xx: f64,
}

impl Moments {
    /// `ConstantMoments`.
    pub fn constant(dt: f64, x: f64) -> Moments {
        let _ = (dt, x);
        todo!()
    }

    /// `Add`: combine over two ranges of time.
    pub fn add(self, other: Moments) -> Moments {
        let _ = other;
        todo!()
    }

    /// `AvgAndStdDev`.
    pub fn avg_and_std_dev(self) -> (f64, f64) {
        todo!()
    }
}

struct State {
    last_time: Duration,
    x: f64,
    moments: Moments,
    min: f64,
    max: f64,
}

/// `integrator` (integrator.go:67).
pub struct Integrator {
    clock: Arc<dyn Clock>,
    state: Mutex<State>,
}

impl Integrator {
    /// `NewNamedIntegrator`.
    pub fn new(clock: Arc<dyn Clock>) -> Integrator {
        let _ = clock;
        todo!()
    }

    pub fn set(&self, x: f64) {
        let _ = (&self.state, x);
        todo!()
    }

    pub fn add(&self, delta_x: f64) {
        let _ = delta_x;
        todo!()
    }

    pub fn get_results(&self) -> IntegratorResults {
        let _ = &self.clock;
        todo!()
    }

    /// Return the results of integrating to now, and reset integration to
    /// start now.
    pub fn reset(&self) -> IntegratorResults {
        todo!()
    }
}

#[cfg(test)]
pub(crate) mod test_clock {
    use super::*;

    /// A settable clock. `after` drops its callback: the integrator and the
    /// borrowing tests never queue a request, so no timer is ever needed.
    pub struct ManualClock {
        now: Mutex<Duration>,
    }

    impl ManualClock {
        pub fn new() -> Arc<ManualClock> {
            Arc::new(ManualClock {
                now: Mutex::new(Duration::ZERO),
            })
        }
        pub fn set(&self, t: Duration) {
            *self.now.lock().unwrap() = t;
        }
        pub fn step(&self, d: Duration) {
            *self.now.lock().unwrap() += d;
        }
    }

    impl Clock for ManualClock {
        fn now(&self) -> Duration {
            *self.now.lock().unwrap()
        }
        fn after(&self, _d: Duration, _f: Box<dyn FnOnce() + Send>) {}
    }
}

#[cfg(test)]
mod tests {
    use super::test_clock::ManualClock;
    use super::*;

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
