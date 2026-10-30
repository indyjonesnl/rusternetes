//! Out of Resource (OOR) Handling
//!
//! Implements kubelet eviction logic for managing node resource exhaustion.
//! When the node runs low on memory or disk space, the kubelet must evict pods
//! to prevent node failure and maintain system stability.
//!
//! Key concepts:
//! - **Eviction Signals**: Memory pressure, disk pressure, PID pressure
//! - **Eviction Thresholds**: Soft and hard thresholds for triggering eviction
//! - **QoS-based Eviction**: Pods are evicted in priority order (BestEffort → Burstable → Guaranteed)
//! - **Resource Usage Ordering**: Within a QoS class, evict based on resource consumption
//!
//! ## Upstream parity references
//!
//! This module mirrors the logic in upstream Kubernetes at:
//! - `pkg/kubelet/eviction/eviction_manager.go::synchronize` — transition-period gate.
//! - `pkg/kubelet/eviction/helpers.go::thresholdsFirstObservedAt`, `nodeConditions`.
//! - `cmd/kubelet/app/options/options.go` — `--eviction-hard`, `--eviction-soft`,
//!   `--eviction-minimum-reclaim`, `--eviction-pressure-transition-period` flags.
//! - `google/cadvisor/fs/fs.go::GetFsInfoForPath` — filesystem stats via `statfs`.

use anyhow::{anyhow, Result};
use rusternetes_common::quantity::Quantity;
use rusternetes_common::resources::{Node, NodeCondition, Pod};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

/// `scheduling.SystemCriticalPriority` = 2 * HighestUserDefinablePriority.
/// Ported from `pkg/apis/scheduling/types.go:32`.
pub const SYSTEM_CRITICAL_PRIORITY: i32 = 2_000_000_000;

/// Mirrors `kubelettypes.IsCriticalPod`
/// (`pkg/kubelet/types/pod_update.go:160-173`): a static pod (config source
/// annotation present and != "api", `IsStaticPod` :144), a mirror pod
/// (`IsMirrorPod` :136) or a pod with priority >= SystemCriticalPriority
/// (`IsCriticalPodBasedOnPriority` :188) is critical.
pub fn is_critical_pod(pod: &Pod) -> bool {
    let ann = pod.metadata.annotations.as_ref();
    if ann
        .and_then(|a| a.get("kubernetes.io/config.source"))
        .is_some_and(|src| src != "api")
    {
        return true;
    }
    if ann.is_some_and(|a| a.contains_key("kubernetes.io/config.mirror")) {
        return true;
    }
    pod.spec
        .as_ref()
        .and_then(|s| s.priority)
        .is_some_and(|p| p >= SYSTEM_CRITICAL_PRIORITY)
}

/// Eviction signals that can trigger pod eviction
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EvictionSignal {
    /// Available memory below threshold
    MemoryAvailable,
    /// Available disk space below threshold (nodefs)
    NodeFsAvailable,
    /// Available inodes below threshold (nodefs)
    NodeFsInodesFree,
    /// Available disk space below threshold (imagefs)
    ImageFsAvailable,
    /// Available inodes below threshold (imagefs)
    ImageFsInodesFree,
    /// Available PIDs below threshold
    PidAvailable,
}

impl EvictionSignal {
    /// Parse the upstream signal name (e.g. `memory.available`).
    pub fn from_upstream_name(name: &str) -> Option<Self> {
        match name {
            "memory.available" => Some(Self::MemoryAvailable),
            "nodefs.available" => Some(Self::NodeFsAvailable),
            "nodefs.inodesFree" => Some(Self::NodeFsInodesFree),
            "imagefs.available" => Some(Self::ImageFsAvailable),
            "imagefs.inodesFree" => Some(Self::ImageFsInodesFree),
            "pid.available" => Some(Self::PidAvailable),
            _ => None,
        }
    }
}

/// Eviction threshold configuration
#[derive(Debug, Clone)]
pub struct EvictionThreshold {
    /// The signal to monitor
    pub signal: EvictionSignal,
    /// Hard threshold (immediate eviction)
    pub hard: Option<EvictionValue>,
    /// Soft threshold (eviction after grace period)
    pub soft: Option<EvictionValue>,
    /// Grace period for soft thresholds
    pub grace_period: Option<Duration>,
}

/// Eviction threshold value (percentage or absolute)
#[derive(Debug, Clone, PartialEq)]
pub enum EvictionValue {
    /// Percentage threshold (e.g., 10.0 means evict when less than 10% available)
    Percentage(f64),
    /// Absolute bytes threshold
    Absolute(u64),
}

/// QoS class for pod eviction priority.
///
/// Re-exported from [`rusternetes_common::qos`], the single port of upstream
/// `ComputePodQOS` — eviction ordering must use the same class the api-server
/// published and the kubelet posts back in `status.qosClass`.
pub use rusternetes_common::qos::QoSClass;

/// Resource statistics for a node
#[derive(Debug, Clone)]
pub struct NodeStats {
    /// Available memory in bytes
    pub memory_available_bytes: u64,
    /// Total memory in bytes
    pub memory_total_bytes: u64,
    /// Available disk space in bytes (nodefs)
    pub nodefs_available_bytes: u64,
    /// Total disk space in bytes (nodefs)
    pub nodefs_total_bytes: u64,
    /// Available inodes (nodefs)
    pub nodefs_inodes_free: u64,
    /// Total inodes (nodefs)
    pub nodefs_inodes_total: u64,
    /// Available PIDs
    pub pid_available: u64,
    /// Total PIDs
    pub pid_total: u64,
    /// The dedicated image filesystem, `None` when the image filesystem shares
    /// the node filesystem's device (upstream `hasImageFs == false`: the
    /// `imagefs.*` signals then read the same numbers as `nodefs.*`).
    pub imagefs: Option<FsUsage>,
}

/// One filesystem's observation (`signalObservation` available/capacity).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FsUsage {
    pub available_bytes: u64,
    pub total_bytes: u64,
    pub inodes_free: u64,
    pub inodes_total: u64,
}

/// Pod resource usage statistics
#[derive(Debug, Clone)]
pub struct PodStats {
    /// Pod name
    pub name: String,
    /// Pod namespace
    pub namespace: String,
    /// Memory usage in bytes
    pub memory_usage_bytes: u64,
    /// Disk usage in bytes
    pub disk_usage_bytes: u64,
    /// QoS class
    pub qos_class: QoSClass,
}

/// Upstream default for `--eviction-pressure-transition-period`.
pub const DEFAULT_TRANSITION_PERIOD: Duration = Duration::from_secs(5 * 60);

/// Resolve the eviction pressure transition period.
///
/// Upstream feeds `kubeCfg.EvictionPressureTransitionPeriod.Duration` into
/// `eviction.Config.PressureTransitionPeriod` (pkg/kubelet/kubelet.go:527).
/// The config-file value is the base (default 5m when unset/zero,
/// pkg/kubelet/apis/config/v1beta1/defaults.go:236-237) and an explicit
/// `--eviction-pressure-transition-period` flag wins over it
/// (cmd/kubelet/app/server.go `kubeletConfigFlagPrecedence`).
pub fn resolve_transition_period(
    flag: Option<&str>,
    config: Option<&crate::config::KubeletConfiguration>,
) -> anyhow::Result<Duration> {
    if let Some(raw) = flag {
        return parse_duration(raw).ok_or_else(|| {
            anyhow::anyhow!("invalid --eviction-pressure-transition-period: '{}'", raw)
        });
    }
    Ok(config
        .map(|c| c.effective_eviction_pressure_transition_period())
        .unwrap_or(DEFAULT_TRANSITION_PERIOD))
}

/// Minimum observation window before a hard threshold actually trips eviction.
/// Mirrors upstream `thresholdsMetGracePeriod` for hard thresholds (which is 0
/// by default but is gated by `thresholdsFirstObservedAt`). We use a small
/// non-zero window so transient blips don't immediately trigger.
const HARD_MIN_OBSERVATION: Duration = Duration::from_secs(10);

/// Minimum interval between repeat "still under pressure" log lines.
const PRESSURE_LOG_INTERVAL: Duration = Duration::from_secs(60);

/// Eviction manager for handling out-of-resource situations
pub struct EvictionManager {
    /// Eviction thresholds configuration
    pub thresholds: Vec<EvictionThreshold>,
    /// Last time soft thresholds were exceeded (for grace period tracking)
    pub soft_threshold_exceeded: HashMap<EvictionSignal, Instant>,
    /// First time each hard signal was observed exceeded (cleared when not exceeded).
    /// Mirrors upstream `thresholdsFirstObservedAt`.
    pub hard_observations: HashMap<EvictionSignal, Instant>,
    /// Last time each hard signal was observed exceeded. Used to enforce
    /// the `EvictionPressureTransitionPeriod`: a signal stays in the active
    /// set until `transition_period` has elapsed since this timestamp.
    /// Mirrors upstream `lastObservations` in `eviction_manager.go`.
    pub last_observed_at: HashMap<EvictionSignal, Instant>,
    /// `--eviction-pressure-transition-period`
    pub transition_period: Duration,
    /// The previously reported set of active hard-pressure signals.
    pub last_pressure_signals: HashSet<EvictionSignal>,
    /// When `last_pressure_signals` last transitioned.
    pub last_pressure_change: Option<Instant>,
    /// Last time we emitted a "still under pressure" info! line.
    pub last_pressure_log: Option<Instant>,
}

/// Default upstream-equivalent thresholds.
/// Matches upstream `cmd/kubelet/app/options/options.go` defaults:
/// `memory.available<100Mi`, `nodefs.available<10%`, `nodefs.inodesFree<5%`,
/// `imagefs.available<15%`, `imagefs.inodesFree<5%`.
fn default_thresholds() -> Vec<EvictionThreshold> {
    vec![
        EvictionThreshold {
            signal: EvictionSignal::MemoryAvailable,
            hard: Some(EvictionValue::Absolute(100 * 1024 * 1024)),
            soft: None,
            grace_period: None,
        },
        EvictionThreshold {
            signal: EvictionSignal::NodeFsAvailable,
            hard: Some(EvictionValue::Percentage(10.0)),
            soft: None,
            grace_period: None,
        },
        EvictionThreshold {
            signal: EvictionSignal::NodeFsInodesFree,
            hard: Some(EvictionValue::Percentage(5.0)),
            soft: None,
            grace_period: None,
        },
        EvictionThreshold {
            signal: EvictionSignal::ImageFsAvailable,
            hard: Some(EvictionValue::Percentage(15.0)),
            soft: None,
            grace_period: None,
        },
        EvictionThreshold {
            signal: EvictionSignal::ImageFsInodesFree,
            hard: Some(EvictionValue::Percentage(5.0)),
            soft: None,
            grace_period: None,
        },
    ]
}

impl EvictionManager {
    /// Create a new eviction manager with upstream-default thresholds.
    pub fn new() -> Self {
        Self::with_config(default_thresholds(), DEFAULT_TRANSITION_PERIOD)
    }

    /// Create with the given thresholds and transition period.
    ///
    /// An empty `thresholds` vec disables the eviction subsystem entirely:
    /// `check_eviction_needed` will return an empty signal set and emit no
    /// node-condition mutations beyond clearing existing pressure conditions.
    pub fn with_config(thresholds: Vec<EvictionThreshold>, transition_period: Duration) -> Self {
        Self {
            thresholds,
            soft_threshold_exceeded: HashMap::new(),
            hard_observations: HashMap::new(),
            last_observed_at: HashMap::new(),
            transition_period,
            last_pressure_signals: HashSet::new(),
            last_pressure_change: None,
            last_pressure_log: None,
        }
    }

    /// True iff the eviction subsystem is disabled (no thresholds configured).
    pub fn is_disabled(&self) -> bool {
        self.thresholds.is_empty()
    }

    /// Check if eviction is needed based on current node statistics.
    ///
    /// Returns the set of signals that should be reported as active pressure,
    /// applying the upstream `EvictionPressureTransitionPeriod` gate:
    /// - A signal that just started exceeding its hard threshold is only
    ///   reported after `HARD_MIN_OBSERVATION` has elapsed (or if it was
    ///   already in the previous active set).
    /// - A signal that has stopped exceeding its hard threshold is kept in
    ///   the active set until `transition_period` has elapsed since the
    ///   last pressure transition. This dampens flapping that would
    ///   otherwise generate watch-event storms.
    pub fn check_eviction_needed(&mut self, stats: &NodeStats) -> Vec<EvictionSignal> {
        self.check_eviction_needed_at(stats, Instant::now())
    }

    /// Test-friendly variant that accepts an explicit `now` to drive the clock.
    pub fn check_eviction_needed_at(
        &mut self,
        stats: &NodeStats,
        now: Instant,
    ) -> Vec<EvictionSignal> {
        if self.thresholds.is_empty() {
            // Disabled: ensure any lingering pressure clears immediately.
            if !self.last_pressure_signals.is_empty() {
                self.last_pressure_signals.clear();
                self.last_pressure_change = Some(now);
            }
            return Vec::new();
        }

        // Phase 1: classify each threshold against the current stats.
        // We clone to avoid &self/&mut self conflicts in is_threshold_exceeded.
        let thresholds = self.thresholds.clone();
        let mut hard_currently_exceeded: HashSet<EvictionSignal> = HashSet::new();
        let mut soft_breached: Vec<EvictionSignal> = Vec::new();
        for threshold in &thresholds {
            if let Some(ref hard) = threshold.hard {
                if Self::compare_threshold(threshold.signal, hard, stats) {
                    hard_currently_exceeded.insert(threshold.signal);
                }
            }
            if let Some(ref soft) = threshold.soft {
                if Self::compare_threshold(threshold.signal, soft, stats) {
                    soft_breached.push(threshold.signal);
                } else {
                    self.soft_threshold_exceeded.remove(&threshold.signal);
                }
            }
        }

        // Phase 2: maintain `hard_observations` — first time each signal was
        // observed crossing the threshold (cleared when it stops) — and
        // `last_observed_at`, the most recent time the signal was exceeded.
        for signal in &hard_currently_exceeded {
            self.hard_observations.entry(*signal).or_insert(now);
            self.last_observed_at.insert(*signal, now);
        }
        let to_clear: Vec<EvictionSignal> = self
            .hard_observations
            .keys()
            .copied()
            .filter(|s| !hard_currently_exceeded.contains(s))
            .collect();
        for signal in to_clear {
            self.hard_observations.remove(&signal);
        }

        // Phase 3: compute the *reported* active set with transition gating.
        let mut active: HashSet<EvictionSignal> = HashSet::new();

        // (a) Newly exceeded signals: include if observation window elapsed
        //     OR if already in the previous active set.
        for signal in &hard_currently_exceeded {
            let first_seen = self.hard_observations.get(signal).copied().unwrap_or(now);
            let observed_long_enough =
                now.saturating_duration_since(first_seen) >= HARD_MIN_OBSERVATION;
            if observed_long_enough || self.last_pressure_signals.contains(signal) {
                active.insert(*signal);
            }
        }

        // (b) Previously-active signals that have recovered: keep them in
        //     the active set until `transition_period` has elapsed since
        //     the LAST observation that the signal was actually exceeded.
        //     Mirrors upstream `nodeConditionsObservedSince` in
        //     `pkg/kubelet/eviction/helpers.go`.
        for signal in &self.last_pressure_signals {
            if !hard_currently_exceeded.contains(signal) {
                if let Some(last_seen) = self.last_observed_at.get(signal).copied() {
                    let elapsed_since_last_seen = now.saturating_duration_since(last_seen);
                    if elapsed_since_last_seen < self.transition_period {
                        active.insert(*signal);
                    }
                }
            }
        }

        // (c) Add soft thresholds that have outlasted their grace period.
        for signal in &soft_breached {
            let exceeded_at = *self.soft_threshold_exceeded.entry(*signal).or_insert(now);
            // Find the threshold's grace period.
            if let Some(grace) = thresholds
                .iter()
                .find(|t| t.signal == *signal)
                .and_then(|t| t.grace_period)
            {
                if now.saturating_duration_since(exceeded_at) >= grace {
                    active.insert(*signal);
                }
            }
        }

        // Phase 4: detect transitions and log accordingly.
        if active != self.last_pressure_signals {
            // A transition. Log the change at info!.
            let entered: Vec<_> = active
                .difference(&self.last_pressure_signals)
                .collect::<Vec<_>>();
            let cleared: Vec<_> = self
                .last_pressure_signals
                .difference(&active)
                .collect::<Vec<_>>();
            if !entered.is_empty() {
                info!(
                    signals = ?entered,
                    "Hard eviction threshold exceeded (entered pressure)"
                );
            }
            if !cleared.is_empty() {
                info!(
                    signals = ?cleared,
                    "Eviction pressure cleared (transition period elapsed)"
                );
            }
            self.last_pressure_signals = active.clone();
            self.last_pressure_change = Some(now);
            self.last_pressure_log = Some(now);
        } else if !active.is_empty() {
            // Stable under pressure. Throttle "still under pressure" logs.
            let should_log = match self.last_pressure_log {
                Some(t) => now.saturating_duration_since(t) >= PRESSURE_LOG_INTERVAL,
                None => true,
            };
            if should_log {
                info!(
                    signals = ?active,
                    "Eviction pressure still active"
                );
                self.last_pressure_log = Some(now);
            } else {
                debug!(signals = ?active, "Eviction pressure still active (rate-limited)");
            }
        }

        // Garbage-collect stale `last_observed_at` entries (older than the
        // transition period AND not currently exceeded).
        let stale: Vec<EvictionSignal> = self
            .last_observed_at
            .iter()
            .filter(|(s, t)| {
                !hard_currently_exceeded.contains(*s)
                    && now.saturating_duration_since(**t) >= self.transition_period
            })
            .map(|(s, _)| *s)
            .collect();
        for signal in stale {
            self.last_observed_at.remove(&signal);
        }

        // Return in stable order for downstream consumers / tests.
        let mut out: Vec<_> = active.into_iter().collect();
        out.sort_by_key(|s| match s {
            EvictionSignal::MemoryAvailable => 0,
            EvictionSignal::NodeFsAvailable => 1,
            EvictionSignal::NodeFsInodesFree => 2,
            EvictionSignal::ImageFsAvailable => 3,
            EvictionSignal::ImageFsInodesFree => 4,
            EvictionSignal::PidAvailable => 5,
        });
        out
    }

    /// Compare current value against threshold. Returns true iff the signal is exceeded
    /// (i.e. available is strictly less than the threshold).
    fn compare_threshold(
        signal: EvictionSignal,
        threshold: &EvictionValue,
        stats: &NodeStats,
    ) -> bool {
        let current = match signal {
            EvictionSignal::MemoryAvailable => stats.memory_available_bytes,
            EvictionSignal::NodeFsAvailable => stats.nodefs_available_bytes,
            EvictionSignal::NodeFsInodesFree => stats.nodefs_inodes_free,
            EvictionSignal::ImageFsAvailable => stats
                .imagefs
                .map_or(stats.nodefs_available_bytes, |f| f.available_bytes),
            EvictionSignal::ImageFsInodesFree => stats
                .imagefs
                .map_or(stats.nodefs_inodes_free, |f| f.inodes_free),
            EvictionSignal::PidAvailable => stats.pid_available,
        };
        let total = match signal {
            EvictionSignal::MemoryAvailable => stats.memory_total_bytes,
            EvictionSignal::NodeFsAvailable => stats.nodefs_total_bytes,
            EvictionSignal::ImageFsAvailable => stats
                .imagefs
                .map_or(stats.nodefs_total_bytes, |f| f.total_bytes),
            EvictionSignal::NodeFsInodesFree => stats.nodefs_inodes_total,
            EvictionSignal::ImageFsInodesFree => stats
                .imagefs
                .map_or(stats.nodefs_inodes_total, |f| f.inodes_total),
            EvictionSignal::PidAvailable => stats.pid_total,
        };

        match threshold {
            EvictionValue::Percentage(pct) => {
                if total > 0 {
                    let available_pct = (current as f64 / total as f64) * 100.0;
                    available_pct < *pct
                } else {
                    false
                }
            }
            EvictionValue::Absolute(bytes) => current < *bytes,
        }
    }

    /// Select pods for eviction based on resource pressure
    pub fn select_pods_for_eviction(
        &self,
        pods: &[Pod],
        pod_stats: &HashMap<String, PodStats>,
        signal: &EvictionSignal,
    ) -> Vec<String> {
        let mut eviction_candidates: Vec<(&Pod, &PodStats)> = pods
            .iter()
            .filter_map(|pod| {
                let key = format!(
                    "{}/{}",
                    pod.metadata.namespace.as_deref().unwrap_or("default"),
                    pod.metadata.name
                );
                pod_stats.get(&key).map(|stats| (pod, stats))
            })
            // Upstream `evictPod` (eviction_manager.go:614-618) refuses with
            // "cannot evict a critical pod" and the caller moves on to the next
            // ranked pod; filtering up front is equivalent.
            .filter(|(pod, _)| {
                if is_critical_pod(pod) {
                    debug!(
                        "Eviction manager: cannot evict a critical pod {}/{}",
                        pod.metadata.namespace.as_deref().unwrap_or("default"),
                        pod.metadata.name
                    );
                    false
                } else {
                    true
                }
            })
            .collect();

        // Sort by eviction priority:
        // 1. QoS class (BestEffort < Burstable < Guaranteed)
        // 2. Resource usage within QoS class
        eviction_candidates.sort_by(|a, b| {
            let qos_cmp = a.1.qos_class.cmp(&b.1.qos_class);
            if qos_cmp != std::cmp::Ordering::Equal {
                return qos_cmp;
            }
            match signal {
                EvictionSignal::MemoryAvailable => {
                    b.1.memory_usage_bytes.cmp(&a.1.memory_usage_bytes)
                }
                EvictionSignal::NodeFsAvailable | EvictionSignal::NodeFsInodesFree => {
                    b.1.disk_usage_bytes.cmp(&a.1.disk_usage_bytes)
                }
                _ => std::cmp::Ordering::Equal,
            }
        });

        eviction_candidates
            .iter()
            .take(5)
            .map(|(pod, _)| {
                format!(
                    "{}/{}",
                    pod.metadata.namespace.as_deref().unwrap_or("default"),
                    pod.metadata.name
                )
            })
            .collect()
    }

    /// Update node conditions based on eviction signals
    pub fn update_node_conditions(
        &self,
        node: &mut Node,
        active_signals: &[EvictionSignal],
    ) -> Result<()> {
        let now = chrono::Utc::now();

        let memory_pressure = active_signals.contains(&EvictionSignal::MemoryAvailable);
        let disk_pressure = active_signals.contains(&EvictionSignal::NodeFsAvailable)
            || active_signals.contains(&EvictionSignal::NodeFsInodesFree)
            || active_signals.contains(&EvictionSignal::ImageFsAvailable)
            || active_signals.contains(&EvictionSignal::ImageFsInodesFree);
        let pid_pressure = active_signals.contains(&EvictionSignal::PidAvailable);

        if let Some(ref mut status) = node.status {
            let conditions = status.conditions.get_or_insert_with(Vec::new);

            Self::update_or_add_condition(
                conditions,
                "MemoryPressure",
                if memory_pressure { "True" } else { "False" },
                if memory_pressure {
                    Some("NodeHasMemoryPressure")
                } else {
                    Some("NodeHasSufficientMemory")
                },
                if memory_pressure {
                    Some("Available memory is below eviction threshold")
                } else {
                    Some("Available memory is sufficient")
                },
                now,
            );

            Self::update_or_add_condition(
                conditions,
                "DiskPressure",
                if disk_pressure { "True" } else { "False" },
                if disk_pressure {
                    Some("NodeHasDiskPressure")
                } else {
                    Some("NodeHasNoDiskPressure")
                },
                if disk_pressure {
                    Some("Available disk space is below eviction threshold")
                } else {
                    Some("Available disk space is sufficient")
                },
                now,
            );

            Self::update_or_add_condition(
                conditions,
                "PIDPressure",
                if pid_pressure { "True" } else { "False" },
                if pid_pressure {
                    Some("NodeHasPIDPressure")
                } else {
                    Some("NodeHasNoPIDPressure")
                },
                if pid_pressure {
                    Some("Available PIDs are below eviction threshold")
                } else {
                    Some("Available PIDs are sufficient")
                },
                now,
            );
        }

        Ok(())
    }

    /// Update or add a node condition
    fn update_or_add_condition(
        conditions: &mut Vec<NodeCondition>,
        condition_type: &str,
        status: &str,
        reason: Option<&str>,
        message: Option<&str>,
        now: chrono::DateTime<chrono::Utc>,
    ) {
        if let Some(condition) = conditions
            .iter_mut()
            .find(|c| c.condition_type == condition_type)
        {
            let status_changed = condition.status != status;
            condition.status = status.to_string();
            condition.last_heartbeat_time = Some(now);
            if status_changed {
                condition.last_transition_time = Some(now);
            }
            condition.reason = reason.map(|s| s.to_string());
            condition.message = message.map(|s| s.to_string());
        } else {
            conditions.push(NodeCondition {
                condition_type: condition_type.to_string(),
                status: status.to_string(),
                last_heartbeat_time: Some(now),
                last_transition_time: Some(now),
                reason: reason.map(|s| s.to_string()),
                message: message.map(|s| s.to_string()),
            });
        }
    }
}

/// `nodeReclaimFunc`: the node-level reclaim actions the eviction manager
/// runs before evicting end-user pods.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeReclaimFunc {
    /// `containerGC.DeleteAllUnusedContainers`
    DeleteAllUnusedContainers,
    /// `imageGC.DeleteUnusedImages`
    DeleteUnusedImages,
}

/// `buildSignalToNodeReclaimFuncs`
/// (`pkg/kubelet/eviction/helpers.go:1202`). The `containerfs.*` signals are
/// not modelled (no `KubeletSeparateDiskGC`), so only the nodefs/imagefs
/// entries are produced.
pub fn build_signal_to_node_reclaim_funcs(
    with_image_fs: bool,
    split_container_image_fs: bool,
) -> HashMap<EvictionSignal, Vec<NodeReclaimFunc>> {
    use EvictionSignal::*;
    use NodeReclaimFunc::*;
    let mut m = HashMap::new();
    // usage of an imagefs is optional
    if with_image_fs && !split_container_image_fs {
        // with an imagefs, nodefs pressure should just delete logs
        m.insert(NodeFsAvailable, vec![]);
        m.insert(NodeFsInodesFree, vec![]);
        // with an imagefs, imagefs pressure should delete unused images
        m.insert(
            ImageFsAvailable,
            vec![DeleteAllUnusedContainers, DeleteUnusedImages],
        );
        m.insert(
            ImageFsInodesFree,
            vec![DeleteAllUnusedContainers, DeleteUnusedImages],
        );
    } else if with_image_fs && split_container_image_fs {
        // image gc on imagefs pressure; containers gc on containerfs pressure
        m.insert(ImageFsAvailable, vec![DeleteUnusedImages]);
        m.insert(ImageFsInodesFree, vec![DeleteUnusedImages]);
        m.insert(NodeFsAvailable, vec![DeleteAllUnusedContainers]);
        m.insert(NodeFsInodesFree, vec![DeleteAllUnusedContainers]);
    } else {
        // without an imagefs, nodefs pressure should delete logs, and unused
        // images; the filesystems share a device so they share reclaim funcs
        for sig in [
            NodeFsAvailable,
            NodeFsInodesFree,
            ImageFsAvailable,
            ImageFsInodesFree,
        ] {
            m.insert(sig, vec![DeleteAllUnusedContainers, DeleteUnusedImages]);
        }
    }
    m
}

/// The `ImageGC` / `ContainerGC` interfaces of the eviction manager plus the
/// summary provider used to re-observe after a reclaim.
#[async_trait::async_trait]
pub trait NodeReclaimer: Send + Sync {
    /// `containerGC.DeleteAllUnusedContainers`
    async fn delete_all_unused_containers(&self) -> Result<()>;
    /// `imageGC.DeleteUnusedImages`
    async fn delete_unused_images(&self) -> Result<()>;
    /// `summaryProvider.Get(ctx, true)` reduced to the node stats.
    async fn observe(&self) -> NodeStats;
}

impl EvictionManager {
    /// True when no configured threshold is met by `stats`
    /// (`len(thresholdsMet(thresholds, observations, true)) == 0`; the
    /// `--eviction-minimum-reclaim` margin is not modelled in this manager).
    pub fn no_thresholds_met(&self, stats: &NodeStats) -> bool {
        !self.thresholds.iter().any(|t| {
            [t.hard.as_ref(), t.soft.as_ref()]
                .into_iter()
                .flatten()
                .any(|v| Self::compare_threshold(t.signal, v, stats))
        })
    }
}

/// `managerImpl.reclaimNodeLevelResources`
/// (`eviction_manager.go:473`): runs the reclaim funcs for `signal` (errors are
/// logged, not fatal), then re-observes; returns true if no threshold is met
/// any more and no pod eviction is required.
pub async fn reclaim_node_level_resources(
    funcs: &[NodeReclaimFunc],
    reclaimer: &dyn NodeReclaimer,
    thresholds_check: impl Fn(&NodeStats) -> bool,
) -> bool {
    for f in funcs {
        let r = match f {
            NodeReclaimFunc::DeleteAllUnusedContainers => {
                reclaimer.delete_all_unused_containers().await
            }
            NodeReclaimFunc::DeleteUnusedImages => reclaimer.delete_unused_images().await,
        };
        if let Err(err) = r {
            info!(%err, "Eviction manager: unexpected error when attempting to reduce resource pressure");
        }
    }
    if funcs.is_empty() {
        return false;
    }
    thresholds_check(&reclaimer.observe().await)
}

impl Default for EvictionManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Parse a comma-separated `--eviction-hard` (or `--eviction-soft`) flag value.
///
/// Format: `<signal><op><value>[,<signal><op><value>...]`.
/// `op` is always `<` (upstream only supports less-than).
/// `value` is `<int>[<unit>]` for absolutes (`100Mi`, `1Gi`) or `<percent>%`.
///
/// An empty string returns an empty map (eviction subsystem disabled).
pub fn parse_eviction_flag(value: &str) -> Result<HashMap<EvictionSignal, EvictionValue>> {
    let mut out = HashMap::new();
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Ok(out);
    }

    for entry in trimmed.split(',') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        // Find the operator (only `<` supported).
        let op_pos = entry
            .find('<')
            .ok_or_else(|| anyhow!("eviction threshold '{}': missing '<' operator", entry))?;
        let signal_str = entry[..op_pos].trim();
        let value_str = entry[op_pos + 1..].trim();
        if signal_str.is_empty() || value_str.is_empty() {
            return Err(anyhow!(
                "eviction threshold '{}': empty signal or value",
                entry
            ));
        }
        let signal = EvictionSignal::from_upstream_name(signal_str)
            .ok_or_else(|| anyhow!("eviction threshold '{}': unknown signal", signal_str))?;
        // `Ok(None)` is upstream's "ignore this statement" (0% / 100%), which
        // must not abort the rest of the flag.
        if let Some(parsed) = parse_threshold_value(signal_str, value_str)? {
            out.insert(signal, parsed);
        }
    }

    Ok(out)
}

/// Parse a duration like `5m`, `30s`, `1h30m`. Returns None if invalid.
pub fn parse_duration(s: &str) -> Option<Duration> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    // Plain seconds as a number.
    if let Ok(n) = s.parse::<u64>() {
        return Some(Duration::from_secs(n));
    }
    // Accept go-style `Ns`, `Nm`, `Nh`, or composed `1h30m`.
    let mut total = Duration::ZERO;
    let mut num = String::new();
    for c in s.chars() {
        if c.is_ascii_digit() {
            num.push(c);
            continue;
        }
        let n: u64 = num.parse().ok()?;
        num.clear();
        match c {
            's' => total += Duration::from_secs(n),
            'm' => total += Duration::from_secs(n * 60),
            'h' => total += Duration::from_secs(n * 3600),
            _ => return None,
        }
    }
    if !num.is_empty() {
        // Trailing bare number = seconds.
        let n: u64 = num.parse().ok()?;
        total += Duration::from_secs(n);
    }
    Some(total)
}

/// Parse a threshold value (`100Mi`, `0.5Gi`, `10%`, `1024`) for `signal`.
///
/// Ports upstream `parseThresholdStatement`
/// (`../kubernetes/pkg/kubelet/eviction/helpers.go:381-424`). Three behaviours
/// come from there and are easy to get wrong:
///
/// - `Ok(None)` means *drop this statement*. Upstream returns `(nil, nil)` for
///   `0%` and `100%` rather than erroring (`helpers.go:387-390`). `100%` is the
///   dangerous one: as a live threshold it is met on every sync, so the node
///   would evict continuously.
/// - The absolute form goes through the full `resource.Quantity` grammar
///   (`helpers.go:410`), which permits a decimal point with every suffix.
/// - The quantity must be strictly positive: upstream rejects
///   `Sign() < 0 || IsZero()` (`helpers.go:414-416`).
pub fn parse_threshold_value(signal: &str, value: &str) -> Result<Option<EvictionValue>> {
    let value = value.trim();

    if let Some(pct) = value.strip_suffix('%') {
        // Ignored outright, before the bounds check.
        if value == "0%" || value == "100%" {
            return Ok(None);
        }
        let p: f64 = pct.trim().parse().map_err(|_| {
            anyhow!("eviction percentage threshold {signal} is not a number: {value}")
        })?;
        // Upstream compares `percentage < 0` / `> 1` on a float, and every
        // comparison against NaN is false — `NaN%` would survive both checks
        // and leave a threshold that can never be met. Reject it here.
        if !p.is_finite() {
            return Err(anyhow!(
                "eviction percentage threshold {signal} must be finite: {value}"
            ));
        }
        if p < 0.0 {
            return Err(anyhow!(
                "eviction percentage threshold {signal} must be >= 0%: {value}"
            ));
        }
        if p > 100.0 {
            return Err(anyhow!(
                "eviction percentage threshold {signal} must be <= 100%: {value}"
            ));
        }
        return Ok(Some(EvictionValue::Percentage(p)));
    }

    let quantity =
        Quantity::parse(value).map_err(|e| anyhow!("eviction threshold {signal}: {value}: {e}"))?;
    if quantity.is_negative() || quantity.is_zero() {
        return Err(anyhow!(
            "eviction threshold {signal} must be positive: {}",
            quantity.canonical_string()
        ));
    }
    // Positive by the check above, so the `u64` conversion cannot fail; an
    // absurd quantity saturates rather than wrapping.
    let bytes = u64::try_from(quantity.value()).unwrap_or(u64::MAX);
    Ok(Some(EvictionValue::Absolute(bytes)))
}

/// Build the threshold list from `--eviction-hard` and `--eviction-soft` maps.
///
/// `hard` and `soft` are the parsed CLI values. An entry present only in `soft`
/// has no hard threshold, and vice versa.
pub fn build_thresholds(
    hard: HashMap<EvictionSignal, EvictionValue>,
    soft: HashMap<EvictionSignal, EvictionValue>,
    soft_grace_periods: HashMap<EvictionSignal, Duration>,
) -> Vec<EvictionThreshold> {
    let mut signals: HashSet<EvictionSignal> = HashSet::new();
    signals.extend(hard.keys().copied());
    signals.extend(soft.keys().copied());

    let mut out = Vec::with_capacity(signals.len());
    for signal in signals {
        out.push(EvictionThreshold {
            signal,
            hard: hard.get(&signal).cloned(),
            soft: soft.get(&signal).cloned(),
            grace_period: soft_grace_periods.get(&signal).copied(),
        });
    }
    out
}

/// Determine QoS class for a pod.
///
/// Thin delegation to [`rusternetes_common::qos::compute_pod_qos`], the single
/// port of upstream `ComputePodQOS`
/// (`pkg/apis/core/v1/helper/qos/qos.go:92-172`). Kept as a named function
/// because eviction reads it per candidate pod, and the name matches what the
/// eviction ordering code (and upstream's `qosComparator`) talks about.
pub fn get_qos_class(pod: &Pod) -> QoSClass {
    rusternetes_common::qos::compute_pod_qos(pod)
}

/// Get filesystem stats for the path that hosts the kubelet's root dir.
///
/// Tries the configured `root_dir` first, then its parent, then `/`. Returns
/// (available_bytes, total_bytes, inodes_free, inodes_total, resolved_path).
///
/// Mirrors `google/cadvisor/fs/fs.go::GetFsInfoForPath` semantics: walk up
/// looking for a directory that exists, then `statfs` it. Unlike sysinfo's
/// per-mount enumeration, this works correctly inside containers because
/// `statvfs` resolves the underlying mount automatically.
pub fn statvfs_for_root_dir(root_dir: &Path) -> Result<(u64, u64, u64, u64, PathBuf)> {
    let candidates = candidate_paths(root_dir);
    let mut last_err: Option<anyhow::Error> = None;
    for path in candidates {
        if !path.exists() {
            continue;
        }
        match rustix::fs::statvfs(&path) {
            Ok(stat) => {
                // `f_frsize` is the fundamental block size in bytes.
                let frsize = stat.f_frsize;
                let available_bytes = stat.f_bavail.saturating_mul(frsize);
                let total_bytes = stat.f_blocks.saturating_mul(frsize);
                let inodes_free = stat.f_favail;
                let inodes_total = stat.f_files;
                return Ok((
                    available_bytes,
                    total_bytes,
                    inodes_free,
                    inodes_total,
                    path,
                ));
            }
            Err(e) => {
                last_err = Some(anyhow!("statvfs({}): {}", path.display(), e));
            }
        }
    }
    Err(last_err.unwrap_or_else(|| anyhow!("no valid path for statvfs")))
}

fn candidate_paths(root_dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::with_capacity(3);
    out.push(root_dir.to_path_buf());
    if let Some(parent) = root_dir.parent() {
        if !parent.as_os_str().is_empty() {
            out.push(parent.to_path_buf());
        }
    }
    out.push(PathBuf::from("/"));
    // Deduplicate while preserving order.
    let mut seen = HashSet::new();
    out.into_iter().filter(|p| seen.insert(p.clone())).collect()
}

/// Get node resource statistics.
///
/// Queries memory via sysinfo and disk via `statvfs` on the kubelet root dir
/// (upstream-parity behavior). PID stats are read from /proc on Linux.
pub fn get_node_stats(root_dir: &Path) -> NodeStats {
    use sysinfo::System;

    let mut sys = System::new_all();
    sys.refresh_all();

    let memory_total_bytes = sys.total_memory();
    let memory_available_bytes = sys.available_memory();

    let (nodefs_available_bytes, nodefs_total_bytes, nodefs_inodes_free, nodefs_inodes_total) =
        match statvfs_for_root_dir(root_dir) {
            Ok((avail, total, ifree, itotal, _)) => (avail, total, ifree, itotal),
            Err(e) => {
                warn!(
                    error = %e,
                    "statvfs failed; assuming abundant disk to avoid spurious eviction pressure"
                );
                // Fail "safe": pretend disk is fine so we don't trigger eviction on a
                // measurement bug. This is the same fallback semantics as upstream
                // when cadvisor returns an error.
                (
                    1024 * 1024 * 1024 * 1024, // 1 TiB
                    1024 * 1024 * 1024 * 1024, // 1 TiB
                    10_000_000,
                    10_000_000,
                )
            }
        };

    let (pid_available, pid_total) = get_pid_stats();

    NodeStats {
        memory_available_bytes,
        memory_total_bytes,
        nodefs_available_bytes,
        nodefs_total_bytes,
        nodefs_inodes_free,
        nodefs_inodes_total,
        pid_available,
        pid_total,
        imagefs: None,
    }
}

/// Log the resolved statvfs path at startup. Call once during kubelet boot.
pub fn log_statvfs_path(root_dir: &Path) {
    match statvfs_for_root_dir(root_dir) {
        Ok((avail, total, ifree, itotal, path)) => {
            info!(
                root_dir = %root_dir.display(),
                resolved = %path.display(),
                available_bytes = avail,
                total_bytes = total,
                inodes_free = ifree,
                inodes_total = itotal,
                "Eviction disk stats: using statvfs"
            );
        }
        Err(e) => {
            warn!(
                root_dir = %root_dir.display(),
                error = %e,
                "Eviction disk stats: statvfs probe failed"
            );
        }
    }
}

/// Get PID statistics
#[cfg(target_os = "linux")]
fn get_pid_stats() -> (u64, u64) {
    let pid_max = std::fs::read_to_string("/proc/sys/kernel/pid_max")
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
        .unwrap_or(32768);

    let pid_used = std::fs::read_dir("/proc")
        .map(|entries| {
            entries
                .filter_map(|e| e.ok())
                .filter(|e| {
                    e.file_name()
                        .to_str()
                        .and_then(|s| s.parse::<u32>().ok())
                        .is_some()
                })
                .count() as u64
        })
        .unwrap_or(100);

    let pid_available = pid_max.saturating_sub(pid_used);

    (pid_available, pid_max)
}

#[cfg(not(target_os = "linux"))]
fn get_pid_stats() -> (u64, u64) {
    use sysinfo::System;

    let mut sys = System::new();
    sys.refresh_processes(sysinfo::ProcessesToUpdate::All, true);

    let pid_used = sys.processes().len() as u64;
    let pid_max: u64 = 32768;
    let pid_available = pid_max.saturating_sub(pid_used);

    (pid_available, pid_max)
}

/// Get pod resource usage statistics from the container runtime.
pub async fn get_pod_stats(pods: &[Pod]) -> HashMap<String, PodStats> {
    get_pod_stats_async(pods).await
}

async fn get_pod_stats_async(pods: &[Pod]) -> HashMap<String, PodStats> {
    let mut stats_map = HashMap::new();

    let socket = std::env::var("CONTAINER_RUNTIME_ENDPOINT")
        .unwrap_or_else(|_| "unix:///run/containerd/containerd.sock".to_string());
    let mut cri = match rusternetes_cri::CriClient::connect(&socket).await {
        Ok(c) => c,
        Err(e) => {
            warn!("Failed to connect to CRI runtime for pod stats: {}", e);
            return stats_map;
        }
    };

    for pod in pods {
        let namespace = pod.metadata.namespace.as_deref().unwrap_or("default");
        let pod_name = &pod.metadata.name;
        let key = format!("{namespace}/{pod_name}");
        let qos_class = get_qos_class(pod);

        // Sum CRI per-container stats for this pod (matched by the pod-uid
        // label): working-set memory + writable-layer disk usage.
        let filter = rusternetes_cri::v1::ContainerStatsFilter {
            label_selector: HashMap::from([(
                crate::labels::POD_UID_LABEL.to_string(),
                pod.metadata.uid.clone(),
            )]),
            ..Default::default()
        };
        let (mut total_memory_bytes, mut total_disk_bytes) = (0u64, 0u64);
        match cri.list_container_stats(Some(filter)).await {
            Ok(stats) => {
                for s in stats {
                    total_memory_bytes += s
                        .memory
                        .and_then(|m| m.working_set_bytes.map(|v| v.value))
                        .unwrap_or(0);
                    total_disk_bytes += s
                        .writable_layer
                        .and_then(|w| w.used_bytes.map(|v| v.value))
                        .unwrap_or(0);
                }
            }
            Err(e) => debug!("Failed to get CRI stats for pod {}: {}", key, e),
        }

        if total_memory_bytes > 0 || total_disk_bytes > 0 {
            stats_map.insert(
                key.clone(),
                PodStats {
                    name: pod_name.clone(),
                    namespace: namespace.to_string(),
                    memory_usage_bytes: total_memory_bytes,
                    disk_usage_bytes: total_disk_bytes,
                    qos_class,
                },
            );
        }
    }

    stats_map
}
