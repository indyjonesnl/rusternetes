//! Prometheus series of the CSIStorageCapacity publisher (#3220, #3548).
//!
//! Port of the `Controller` collector in kubernetes-csi/external-provisioner
//! v6.4.0 `pkg/capacity/capacity.go:122-144` (descriptors) and `:743-808`
//! (`CollectWithStability`, `getObjectsGoal/Current/Obsolete`). As upstream,
//! the values are computed lazily at scrape time ([`gather`]) from the
//! controller's live state, not stored after a reconcile pass.
//!
//! Deviation: upstream runs one driver per process and has no labels; this
//! process may run one publisher per CSI driver, so each series carries a
//! `driver_name` label to keep them apart.

use prometheus::{Encoder, IntGaugeVec, Opts, Registry, TextEncoder};
use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};

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
    // capacity.go:122-143
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

/// The three gauge values at one instant.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CapacityStats {
    /// `getObjectsGoal` (:773): work items that are meant to have an object.
    pub goal: i64,
    /// `getObjectsCurrent` (:780): work items that have an object.
    pub current: i64,
    /// `getObjectsObsolete` (:796): managed objects that match no work item.
    pub obsolete: i64,
}

/// A registered collector: `None` once the controller it reads is gone.
type Collector = Box<dyn Fn() -> Option<CapacityStats> + Send + Sync>;

static COLLECTORS: LazyLock<Mutex<HashMap<String, Collector>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// `legacyregistry.CustomMustRegister(capacityController)`
/// (csi-provisioner.go:545), keyed by driver: `collect` is called on each
/// scrape. Registering a driver again replaces its collector.
pub fn register(driver: &str, collect: impl Fn() -> Option<CapacityStats> + Send + Sync + 'static) {
    COLLECTORS
        .lock()
        .unwrap()
        .insert(driver.to_string(), Box::new(collect));
}

/// The registry in Prometheus text exposition format, with every registered
/// collector evaluated now (`CollectWithStability`).
pub fn gather() -> String {
    let s = &*SERIES;
    COLLECTORS.lock().unwrap().retain(|driver, collect| {
        let Some(stats) = collect() else {
            return false;
        };
        s.goal.with_label_values(&[driver]).set(stats.goal);
        s.current.with_label_values(&[driver]).set(stats.current);
        s.obsolete.with_label_values(&[driver]).set(stats.obsolete);
        true
    });
    let mut buf = Vec::new();
    TextEncoder::new()
        .encode(&s.registry.gather(), &mut buf)
        .expect("text encode");
    String::from_utf8(buf).expect("utf8")
}
