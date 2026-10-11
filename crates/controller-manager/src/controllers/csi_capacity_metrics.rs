//! Prometheus series of the CSIStorageCapacity publisher (#3220).
//!
//! Port of the `Controller` collector in kubernetes-csi/external-provisioner
//! v6.4.0 `pkg/capacity/capacity.go:123-145` (descriptors) and `:743-807`
//! (`CollectWithStability`, `getObjectsGoal/Current/Obsolete`).
//!
//! Deviations: upstream computes the values lazily at scrape time under
//! `capacitiesLock`; the level-triggered publisher instead stores the values
//! after each reconcile pass. Upstream runs one driver per process and has no
//! labels; this process may run one publisher per CSI driver, so each series
//! carries a `driver_name` label to keep them apart.

use prometheus::{Encoder, IntGaugeVec, Opts, Registry, TextEncoder};
use std::sync::LazyLock;

const LABEL: &[&str] = &["driver_name"];

struct Series {
    registry: Registry,
    goal: IntGaugeVec,
    current: IntGaugeVec,
    obsolete: IntGaugeVec,
}

fn gauge(registry: &Registry, name: &str, help: &str) -> IntGaugeVec {
    let g = IntGaugeVec::new(Opts::new(name, help), LABEL).expect("valid opts");
    registry
        .register(Box::new(g.clone()))
        .expect("register once");
    g
}

static SERIES: LazyLock<Series> = LazyLock::new(|| {
    let registry = Registry::new();
    // capacity.go:123-143
    let goal = gauge(
        &registry,
        "csistoragecapacities_desired_goal",
        "Number of CSIStorageCapacity objects that are supposed to be managed automatically.",
    );
    let current = gauge(
        &registry,
        "csistoragecapacities_desired_current",
        "Number of CSIStorageCapacity objects that exist and are supposed to be managed automatically.",
    );
    let obsolete = gauge(
        &registry,
        "csistoragecapacities_obsolete",
        "Number of CSIStorageCapacity objects that exist and will be deleted automatically. Objects that exist and may need an update are not considered obsolete and therefore not included in this value.",
    );
    Series {
        registry,
        goal,
        current,
        obsolete,
    }
});

/// The three gauge values of one reconcile pass.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CapacityStats {
    /// `getObjectsGoal` (:773): work items that are meant to have an object.
    pub goal: i64,
    /// `getObjectsCurrent` (:780): work items that have an object.
    pub current: i64,
    /// `getObjectsObsolete` (:796): managed objects that match no work item.
    pub obsolete: i64,
}

/// Store the values of a finished pass for `driver`.
pub fn publish(driver: &str, stats: CapacityStats) {
    let s = &*SERIES;
    s.goal.with_label_values(&[driver]).set(stats.goal);
    s.current.with_label_values(&[driver]).set(stats.current);
    s.obsolete.with_label_values(&[driver]).set(stats.obsolete);
}

/// The registry in Prometheus text exposition format.
pub fn gather() -> String {
    let mut buf = Vec::new();
    TextEncoder::new()
        .encode(&SERIES.registry.gather(), &mut buf)
        .expect("text encode");
    String::from_utf8(buf).expect("utf8")
}
