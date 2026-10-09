//! API Priority and Fairness: request classification and seat accounting.
//!
//! BOUNDED SLICE of `staging/src/k8s.io/apiserver/pkg/util/flowcontrol`
//! (release-1.35). Ported:
//!
//! - `rule.go` (`matchesFlowSchema` .. `containsString`): FlowSchema matching.
//! - `apf_controller.go` `digestFlowSchemasLocked` (:754-790): drop schemas
//!   whose priority-level ref is missing, sort by (matchingPrecedence, name)
//!   (`pkg/apis/flowcontrol/util/helpers.go:35-42`), re-add the mandatory
//!   `exempt` first / `catch-all` last when absent.
//! - `apf_controller.go` `finishQueueSetReconfigsLocked` (:846-905):
//!   `ceil(serverCL * shares / shareSum)` nominal concurrency, with
//!   `serverCL = --max-requests-inflight + --max-mutating-requests-inflight`
//!   (`server/config.go:443-444`: 400 + 200).
//! - `priority-and-fairness.go` caller contract: `exempt` levels never wait;
//!   `Reject` => 429 immediately; `Queue` => bounded wait then 429
//!   (`tooManyRequests`, priority-and-fairness.go:377-381).
//!
//! - `apf_controller.go` `queueSetCompleterForPL` (:916-955) and
//!   `startRequest` (:1022-1079): one `QueueSet` per priority level
//!   (`flow_control_queueset.rs`), built from `LimitResponse.Queuing`
//!   (`Reject` => `DesiredNumQueues` 0, i.e. queueless; `Exempt` => -1),
//!   re-configured in place (`BeginConfigChange`) on every digest so queued and
//!   executing requests survive a reload; `hashFlowID` (:1131-1140) with the
//!   flow distinguisher computed only when the level has more than one queue.
//!
//! - Borrowing (#2743): `finishQueueSetReconfigsLocked` min/max/initial
//!   currentCL (:859-900) and `updateBorrowingLocked` (:399-496), driven every
//!   `borrowingAdjustmentPeriod` by [`FlowControlEngine::update_borrowing`]
//!   from smoothed seat-demand statistics (`seatDemandStats`, :255-275) fed by
//!   each queueset's seat-demand integrator, and
//!   `computeConcurrencyAllocation` (`flow_control_conc_alloc.rs`).
//!
//! DELIBERATE DEVIATIONS (tracked as follow-up issues): no metrics
//! (`ConcurrencyDenominator`, ratioed gauges,
//! `NotePriorityLevelConcurrencyAdjustment`: #2809) and no FlowSchema status
//! updates.
//!
//! Quiescing (#2769): `digest` ports `digestNewPLsLocked` (broken specs are
//! ignored), `processOldPLsLocked` (an undesired level that is still busy is
//! kept, `quiescing`, with its old queueset and share until idle; a level
//! whose new spec is broken is treated the same way) and `maybeReap`
//! (`FlowControlEngine::maybe_reap`, to be called from the filter when a
//! request finishes with its queueset idle). `numPending` is replaced by
//! holding the config read lock across `start_request`. FlowSchemas that
//! reference an absent mandatory level are kept (upstream marks them dangling
//! because it imagines the level only afterwards); the mandatory FlowSchemas
//! are equivalent either way.

use rusternetes_common::resources::flowcontrol::{
    FlowDistinguisherMethodType, FlowSchema, FlowSchemaSubject, LimitResponseType,
    NonResourcePolicyRule, PolicyRulesWithSubjects, PriorityLevelConfiguration, PriorityLevelType,
    ResourcePolicyRule, SubjectKind,
};
use rusternetes_common::validation::flowcontrol_bootstrap::{
    mandatory_flow_schema, mandatory_priority_level_configuration,
};
use rusternetes_storage::{build_key, Storage};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use crate::flow_control_conc_alloc::{compute_concurrency_allocation, AllocProblemItem};
use crate::flow_control_integrator::{Integrator, IntegratorResults};
use crate::flow_control_metrics as fcmetrics;
use crate::flow_control_queueset::{
    Clock, DispatchingConfig, Execution, QueueSet, QueuingConfig, RealClock, RejectReason,
    WorkEstimate,
};
use sha2::{Digest, Sha256};
use tracing::{error, warn};

/// `--max-requests-inflight` default (`server/config.go:443`).
pub const DEFAULT_MAX_REQUESTS_IN_FLIGHT: i64 = 400;
/// `--max-mutating-requests-inflight` default (`server/config.go:444`).
pub const DEFAULT_MAX_MUTATING_REQUESTS_IN_FLIGHT: i64 = 200;
/// `priorityLevelMaxSeatsPercent` (apf_controller.go:63).
const PRIORITY_LEVEL_MAX_SEATS_PERCENT: f64 = 0.15;

/// `borrowingAdjustmentPeriod` (apf_controller.go:79).
pub const BORROWING_ADJUSTMENT_PERIOD: Duration = Duration::from_secs(10);
/// Default per-level `NominalConcurrencyShares` (`v1/defaults.go`).
const DEFAULT_SHARES: i32 = 30;
/// `Retry-After` value sent with a 429.
pub const RETRY_AFTER_SECONDS: &str = "1";

/// `RequestDigest` (`apf_filter.go`) reduced to what rule matching reads.
#[derive(Debug, Clone, Default)]
pub struct RequestDigest {
    pub user_name: String,
    pub groups: Vec<String>,
    pub is_resource_request: bool,
    pub verb: String,
    pub api_group: String,
    pub resource: String,
    pub subresource: String,
    pub namespace: String,
    pub path: String,
}

// ---- rule.go ----

/// rule.go `containsString`.
fn contains_string(x: &str, list: &[String], wildcard: &str) -> bool {
    if list.len() == 1 && list[0] == wildcard {
        return true;
    }
    list.iter().any(|y| y == x)
}

/// rule.go `serviceAccountMatchesNamespace`.
fn sa_matches_namespace(namespace: &str, username: &str) -> bool {
    match username
        .strip_prefix("system:serviceaccount:")
        .and_then(|r| r.strip_prefix(namespace))
    {
        Some(rest) => rest.starts_with(':'),
        None => false,
    }
}

/// rule.go `matchesSubject`.
fn matches_subject(d: &RequestDigest, s: &FlowSchemaSubject) -> bool {
    match s.kind {
        SubjectKind::User => s
            .user
            .as_ref()
            .is_some_and(|u| u.name == "*" || u.name == d.user_name),
        SubjectKind::Group => match &s.group {
            None => false,
            Some(g) => g.name == "*" || d.groups.contains(&g.name),
        },
        SubjectKind::ServiceAccount => match &s.service_account {
            None => false,
            Some(sa) if sa.name == "*" => sa_matches_namespace(&sa.namespace, &d.user_name),
            Some(sa) => {
                d.user_name == format!("system:serviceaccount:{}:{}", sa.namespace, sa.name)
            }
        },
        _ => false,
    }
}

fn matches_resource_rule(d: &RequestDigest, r: &ResourcePolicyRule) -> bool {
    if !contains_string(&d.verb, &r.verbs, "*") {
        return false;
    }
    let rs = if d.subresource.is_empty() {
        d.resource.clone()
    } else {
        format!("{}/{}", d.resource, d.subresource)
    };
    if !contains_string(&rs, &r.resources, "*") {
        return false;
    }
    if !contains_string(&d.api_group, &r.api_groups, "*") {
        return false;
    }
    if d.namespace.is_empty() {
        return r.cluster_scope.unwrap_or(false);
    }
    contains_string(&d.namespace, r.namespaces.as_deref().unwrap_or(&[]), "*")
}

/// rule.go `matchPolicyRuleNonResourceURL`.
fn matches_non_resource_url(rule_urls: &[String], path: &str) -> bool {
    for rule_path in rule_urls {
        if rule_path == "*" || rule_path == path {
            return true;
        }
        let mut prefix = rule_path.strip_suffix('*').unwrap_or(rule_path).to_string();
        if !prefix.ends_with('/') {
            prefix.push('/');
        }
        if path.starts_with(&prefix) {
            return true;
        }
    }
    false
}

fn matches_non_resource_rule(d: &RequestDigest, r: &NonResourcePolicyRule) -> bool {
    contains_string(&d.verb, &r.verbs, "*")
        && matches_non_resource_url(&r.non_resource_urls, &d.path)
}

fn matches_policy_rule(d: &RequestDigest, p: &PolicyRulesWithSubjects) -> bool {
    if !p.subjects.iter().any(|s| matches_subject(d, s)) {
        return false;
    }
    if d.is_resource_request {
        p.resource_rules
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .any(|r| matches_resource_rule(d, r))
    } else {
        p.non_resource_rules
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .any(|r| matches_non_resource_rule(d, r))
    }
}

/// rule.go `matchesFlowSchema`.
pub fn matches_flow_schema(d: &RequestDigest, fs: &FlowSchema) -> bool {
    fs.spec
        .rules
        .as_deref()
        .unwrap_or(&[])
        .iter()
        .any(|p| matches_policy_rule(d, p))
}

// ---- configuration digest ----

struct LevelState {
    /// `priorityLevelState.pl`; an undesired (quiescing) level keeps the spec
    /// it had when it was last desired.
    pl: PriorityLevelConfiguration,
    exempt: bool,
    /// `nominalCL`: the concurrency limit configured by shares.
    nominal_cl: usize,
    /// `minCL`: the nominal limit less the lendable amount.
    min_cl: usize,
    /// `maxCL`: the nominal limit plus the amount that may be borrowed.
    max_cl: usize,
    /// `currentCL` and `seatDemandStats`, adjusted by `updateBorrowingLocked`
    /// (always under the config write lock; the mutex only satisfies `Arc`).
    borrow: Mutex<BorrowDynamic>,
    /// `QueuingConfig.DesiredNumQueues` (startRequest reads `Queues` for it).
    num_queues: isize,
    queues: Arc<QueueSet>,
    /// `priorityLevelState.quiescing` (apf_controller.go:215-217).
    quiescing: bool,
}

/// The dynamic part of a priority level: `currentCL` and `seatDemandStats`.
#[derive(Clone, Copy, Debug, Default)]
struct BorrowDynamic {
    current_cl: usize,
    stats: SeatDemandStats,
}

/// `seatDemandSmoothingCoefficient` (apf_controller.go:88): half-life of the
/// smoothing is 5 minutes at the 10s adjustment period.
const SEAT_DEMAND_SMOOTHING_COEFFICIENT: f64 = 0.977;

/// `seatDemandStats` (apf_controller.go:255): derived from periodically
/// examining the seat-demand integrator.
#[derive(Clone, Copy, Debug, Default)]
struct SeatDemandStats {
    avg: f64,
    std_dev: f64,
    high_watermark: usize,
    smoothed: f64,
}

impl SeatDemandStats {
    /// `seatDemandStats.update` (apf_controller.go:262-274).
    fn update(&mut self, obs: &IntegratorResults) {
        self.high_watermark = obs.max.round().max(0.0) as usize;
        if obs.duration <= 0.0 {
            return;
        }
        let deviation = if obs.deviation.is_nan() {
            0.0
        } else {
            obs.deviation
        };
        self.avg = obs.average;
        self.std_dev = deviation;
        let envelope = obs.average + deviation;
        self.smoothed = envelope.max(
            SEAT_DEMAND_SMOOTHING_COEFFICIENT * self.smoothed
                + (1.0 - SEAT_DEMAND_SMOOTHING_COEFFICIENT) * envelope,
        );
    }
}

/// `relDiff` (apf_controller.go:1142).
fn rel_diff(x: f64, y: f64) -> f64 {
    let den = x.abs().max(y.abs());
    if den == 0.0 {
        0.0
    } else {
        (x - y).abs() / den
    }
}

struct Config {
    /// `nominalCLSum` (apf_controller.go:193): the sum of the levels' nominal
    /// limits (`meal.maxExecutingRequests`).
    nominal_cl_sum: usize,
    flow_schemas: Vec<FlowSchema>,
    levels: HashMap<String, LevelState>,
    /// The objects this config was digested from, so a reap can re-digest
    /// without going back to storage.
    inputs: (Vec<PriorityLevelConfiguration>, Vec<FlowSchema>),
}

/// Result of classification.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Classification {
    pub flow_schema: String,
    pub priority_level: String,
    pub flow_distinguisher: String,
    /// `FlowSchema.metadata.uid`, sent as
    /// `X-Kubernetes-PF-FlowSchema-UID` (priority-and-fairness.go:354-361).
    pub flow_schema_uid: String,
    /// `PriorityLevelConfiguration.metadata.uid`, sent as
    /// `X-Kubernetes-PF-PriorityLevel-UID`.
    pub priority_level_uid: String,
    /// `!nonMutatingRequestVerbs.Has(requestInfo.Verb)`
    /// (server/filters/maxinflight.go:46, priority-and-fairness.go:143):
    /// anything but get/list/watch. Feeds `read_vs_write_current_requests`.
    pub is_mutating: bool,
}

fn shares_of(pl: &PriorityLevelConfiguration) -> i32 {
    // plSpecCommons (apf_controller.go:1152-1162).
    match (&pl.spec.limited, &pl.spec.exempt) {
        (Some(l), _) => l.nominal_concurrency_shares.unwrap_or(DEFAULT_SHARES),
        (None, Some(e)) => e.nominal_concurrency_shares.unwrap_or(0),
        _ => 0,
    }
}

/// `queueSetCompleterForPL` (apf_controller.go:916-955): the queueset config
/// of a priority level.
fn queuing_config_for_pl(pl: &PriorityLevelConfiguration) -> QueuingConfig {
    let mut q = QueuingConfig {
        name: pl.metadata.name.clone(),
        ..Default::default()
    };
    if pl.spec.limited.is_some() {
        if let Some(qc) = pl
            .spec
            .limited
            .as_ref()
            .and_then(|l| l.limit_response.as_ref())
            .and_then(|lr| lr.queuing.as_ref())
        {
            q.desired_num_queues = qc.queues as isize;
            q.queue_length_limit = qc.queue_length_limit as isize;
            q.hand_size = qc.hand_size as isize;
        }
    } else {
        q.desired_num_queues = -1;
    }
    q
}

/// `hashFlowID` (apf_controller.go:1131-1140): the first 8 bytes of
/// `sha256(fsName || 0x00 || flowDistinguisher)`, little endian.
pub fn hash_flow_id(fs_name: &str, flow_distinguisher: &str) -> u64 {
    let mut h = Sha256::new();
    h.update(fs_name.as_bytes());
    h.update([0u8]);
    h.update(flow_distinguisher.as_bytes());
    let sum = h.finalize();
    u64::from_le_bytes(sum[..8].try_into().expect("sha256 is 32 bytes"))
}

/// `queueSetCompleterForPL`'s validation (apf_controller.go:916-955, the
/// `errors.New(...)` branches and `BeginConstruction`/`BeginConfigChange`'s
/// `checkConfig`): `Err` means the object is "broken" and is ignored.
fn validate_pl(pl: &PriorityLevelConfiguration) -> Result<(), String> {
    let is_limited = matches!(pl.spec.type_, PriorityLevelType::Limited);
    let is_exempt = matches!(pl.spec.type_, PriorityLevelType::Exempt);
    if is_limited != pl.spec.limited.is_some() {
        return Err("broken union structure at the top, for Limited".into());
    }
    if is_exempt != pl.spec.exempt.is_some() {
        return Err("broken union structure at the top, for Exempt".into());
    }
    if is_exempt != (pl.metadata.name == "exempt") {
        return Err("non-alignment between name and type".into());
    }
    if let Some(lr) = pl
        .spec
        .limited
        .as_ref()
        .and_then(|l| l.limit_response.as_ref())
    {
        let reject = matches!(lr.type_, LimitResponseType::Reject);
        if reject != lr.queuing.is_none() {
            return Err("broken union structure for limit response".into());
        }
    }
    QueueSet::validate_config(&queuing_config_for_pl(pl)).map_err(|e| {
        format!(
            "priority level {:?} has QueuingConfiguration {:?}, which is invalid: {}",
            pl.metadata.name,
            queuing_config_for_pl(pl),
            e
        )
    })
}

/// Port of `lockAndDigestConfigObjects` (apf_controller.go:679-712):
/// `digestNewPLsLocked`, `digestFlowSchemasLocked`, `processOldPLsLocked`,
/// the `imaginePL` of missing mandatory levels, and
/// `finishQueueSetReconfigsLocked`.
///
/// Deviation (Rust expression, same mechanism): upstream's `numPending`
/// counts goroutines between `Match` and `StartRequest`. Here
/// [`FlowControlEngine::execute`] holds the config read lock across the
/// synchronous `start_request`, and this function runs under the write lock,
/// so a level seen idle here cannot receive a request from an older snapshot;
/// `numPending` is therefore always 0.
fn digest(
    pls: Vec<PriorityLevelConfiguration>,
    fss: Vec<FlowSchema>,
    server_cl: i64,
    prev: Option<&Config>,
    clock: &Arc<dyn Clock>,
) -> Config {
    /// A level of `meal.newPLStates` before `finishQueueSetReconfigsLocked`.
    struct Pending {
        pl: PriorityLevelConfiguration,
        queues: Option<Arc<QueueSet>>,
        quiescing: bool,
        /// `currentCL` and stats of the level being retained.
        borrow: Option<BorrowDynamic>,
    }
    let mut new_states: HashMap<String, Pending> = HashMap::new();
    let mut share_sum = 0f64;

    // digestNewPLsLocked (:728-752): pretend broken ones do not exist.
    for pl in &pls {
        if let Err(e) = validate_pl(pl) {
            warn!(
                "Ignoring PriorityLevelConfiguration object {} because its spec is broken: {}",
                pl.metadata.name, e
            );
            continue;
        }
        let old = prev.and_then(|c| c.levels.get(&pl.metadata.name));
        if old.is_some_and(|o| o.quiescing) {
            // it was undesired, but no longer (:737-740)
            tracing::debug!(
                "Priority level {:?} was undesired and has become desired again",
                pl.metadata.name
            );
        }
        share_sum += shares_of(pl) as f64;
        new_states.insert(
            pl.metadata.name.clone(),
            Pending {
                pl: pl.clone(),
                queues: old.map(|o| o.queues.clone()),
                quiescing: false,
                borrow: old.map(|o| *o.borrow.lock().unwrap()),
            },
        );
    }
    let have_exempt = new_states.contains_key("exempt");
    let have_catch_all = new_states.contains_key("catch-all");

    // digestFlowSchemasLocked (:754-790), done before holding over old
    // priority levels so requests stop going to those levels: drop FlowSchemas
    // with a missing or broken priority-level ref.
    let mut seq: Vec<FlowSchema> = fss
        .iter()
        .filter(|f| {
            new_states.contains_key(&f.spec.priority_level_configuration.name)
                || matches!(
                    f.spec.priority_level_configuration.name.as_str(),
                    "exempt" | "catch-all"
                )
        })
        .cloned()
        .collect();
    // FlowSchemaSequence.Less (helpers.go:35-42).
    seq.sort_by(|a, b| {
        a.spec
            .matching_precedence
            .cmp(&b.spec.matching_precedence)
            .then_with(|| a.metadata.name.cmp(&b.metadata.name))
    });
    if !seq.iter().any(|f| f.metadata.name == "exempt") {
        seq.insert(
            0,
            mandatory_flow_schema("exempt").expect("mandatory exempt"),
        );
    }
    if !seq.iter().any(|f| f.metadata.name == "catch-all") {
        seq.push(mandatory_flow_schema("catch-all").expect("mandatory catch-all"));
    }

    // processOldPLsLocked (:805-865). Keep the old levels that supply
    // mandatory behavior or are still busy; drop idle ones; start
    // quiescing the busy ones.
    let mut have_exempt_pl = have_exempt;
    let mut have_catch_all_pl = have_catch_all;
    if let Some(prev) = prev {
        for (name, old) in &prev.levels {
            if new_states.contains_key(name) {
                continue; // still desired and already updated
            }
            let mut quiescing = old.quiescing;
            let mandatory_lacking = (name == "exempt" && !have_exempt_pl)
                || (name == "catch-all" && !have_catch_all_pl);
            if mandatory_lacking {
                // Retaining mandatory priority level despite lack of API
                // object (:815-818).
            } else {
                if old.queues.is_idle() {
                    // Removing undesired priority level (:819-826).
                    continue;
                }
                if !quiescing {
                    tracing::debug!("Priority level {:?} became undesired", name);
                    quiescing = true;
                }
            }
            // Lingering levels keep their share so their queues continue
            // to drain (:850-856).
            share_sum += shares_of(&old.pl) as f64;
            have_exempt_pl = have_exempt_pl || name == "exempt";
            have_catch_all_pl = have_catch_all_pl || name == "catch-all";
            new_states.insert(
                name.clone(),
                Pending {
                    pl: old.pl.clone(),
                    queues: Some(old.queues.clone()),
                    quiescing,
                    borrow: Some(*old.borrow.lock().unwrap()),
                },
            );
        }
    }

    // Supply missing mandatory levels (:696-701, imaginePL :991-1015).
    for (name, have) in [("exempt", have_exempt_pl), ("catch-all", have_catch_all_pl)] {
        if !have {
            let pl = mandatory_priority_level_configuration(name).expect("mandatory level");
            share_sum += shares_of(&pl) as f64;
            new_states.insert(
                name.to_string(),
                Pending {
                    pl,
                    queues: None,
                    quiescing: false,
                    borrow: None,
                },
            );
        }
    }

    // finishQueueSetReconfigsLocked (:846-912).
    let mut levels = HashMap::new();
    let mut nominal_cl_sum = 0usize;
    let mut max_waiting_requests = 0isize;
    for (name, st) in new_states {
        let exempt = matches!(st.pl.spec.type_, PriorityLevelType::Exempt);
        let cl = if share_sum > 0.0 {
            ((server_cl as f64) * shares_of(&st.pl) as f64 / share_sum).ceil() as usize
        } else {
            0
        };
        let (lendable_percent, borrowing_limit_percent) = lend_borrow_percents(&st.pl);
        let lendable_cl = lendable_percent
            .map(|p| ((cl as f64) * (p as f64) / 100.0).round() as usize)
            .unwrap_or(0);
        let borrowing_cl = match borrowing_limit_percent {
            Some(p) => ((cl as f64) * (p as f64) / 100.0).round() as usize,
            None => server_cl.max(0) as usize,
        };
        let (min_cl, max_cl) = (cl.saturating_sub(lendable_cl), cl + borrowing_cl);
        nominal_cl_sum += cl;
        // `metrics.SetPriorityLevelConfiguration` (apf_controller.go:874).
        fcmetrics::set_priority_level_configuration(&name, cl as i64, min_cl as i64, max_cl as i64);
        // Introducing queues starts with currentCL = nominalCL - lendableCL/2
        // and no demand history (:897-900); retained ones keep theirs.
        let borrow = match st.borrow {
            Some(b) if st.queues.is_some() => b,
            _ => BorrowDynamic {
                current_cl: cl - lendable_cl / 2,
                stats: SeatDemandStats::default(),
            },
        };
        let qcfg = queuing_config_for_pl(&st.pl);
        let dcfg = DispatchingConfig {
            concurrency_limit: borrow.current_cl,
        };
        let num_queues = qcfg.desired_num_queues;
        if num_queues > 0 {
            // `meal.maxWaitingRequests += Queues * QueueLengthLimit` (:882).
            max_waiting_requests += num_queues * qcfg.queue_length_limit;
        }
        // validate_pl (or an earlier digest) already approved this config.
        let queues = match st.queues {
            Some(q) => {
                q.set_configuration(qcfg, dcfg)
                    .expect("queueing config was validated");
                q
            }
            None => {
                QueueSet::new(clock.clone(), qcfg, dcfg).expect("queueing config was validated")
            }
        };
        levels.insert(
            name,
            LevelState {
                pl: st.pl,
                exempt,
                nominal_cl: cl,
                min_cl,
                max_cl,
                borrow: Mutex::new(borrow),
                num_queues,
                queues,
                quiescing: st.quiescing,
            },
        );
    }
    // The read-vs-write denominators (apf_controller.go:705-708).
    fcmetrics::set_read_write_denominators(max_waiting_requests as f64, nominal_cl_sum as f64);
    // `meal.cfgCtlr.nominalCLSum = meal.maxExecutingRequests;
    // updateBorrowingLocked(false, newPLStates)` (:909-910).
    update_borrowing_locked(&levels, nominal_cl_sum, server_cl);
    Config {
        nominal_cl_sum,
        flow_schemas: seq,
        levels,
        inputs: (pls, fss),
    }
}

/// `plSpecCommons` (apf_controller.go:1152): the (LendablePercent,
/// BorrowingLimitPercent) of a level; an exempt level has no borrowing limit.
fn lend_borrow_percents(pl: &PriorityLevelConfiguration) -> (Option<i32>, Option<i32>) {
    match (&pl.spec.limited, &pl.spec.exempt) {
        (Some(l), _) => (l.lendable_percent, l.borrowing_limit_percent),
        (None, Some(e)) => (e.lendable_percent, None),
        _ => (None, None),
    }
}

/// `updateBorrowingLocked` (apf_controller.go:399-496): derive every level's
/// `currentCL` from its smoothed seat demand and the server's total
/// concurrency, and impose it on the level's queueset (the completer
/// re-creation upstream does is `set_configuration` here).
/// `qll` of `queueSet.setConfiguration` (queueset.go:276-282).
fn queue_capacity(q: &QueuingConfig) -> isize {
    let mut qll = q.queue_length_limit.max(1);
    if q.desired_num_queues > 0 {
        qll *= q.desired_num_queues;
    }
    qll
}

fn update_borrowing_locked(
    levels: &HashMap<String, LevelState>,
    nominal_cl_sum: usize,
    server_cl: i64,
) {
    let mut items: Vec<AllocProblemItem> = Vec::with_capacity(levels.len());
    let mut non_exempt_names: Vec<&str> = Vec::with_capacity(levels.len());
    let mut idx_of_non_exempt: HashMap<&str, usize> = HashMap::new();
    // minCurrentCL of the exempt levels
    let mut ccl_of_exempt: HashMap<&str, usize> = HashMap::new();
    // sums over non-exempt levels
    let (mut min_cl_sum, mut min_current_cl_sum) = (0usize, 0usize);
    let mut remaining_server_cl = nominal_cl_sum as i64;
    for (name, l) in levels {
        let mut b = l.borrow.lock().unwrap();
        let obs = l.queues.seat_demand().reset();
        b.stats.update(&obs);
        if l.exempt {
            let min_current_cl = l.min_cl.max(b.stats.high_watermark);
            ccl_of_exempt.insert(name, min_current_cl);
            remaining_server_cl -= min_current_cl as i64;
        } else {
            // Lower bound on this level's adjusted limit is the lesser of its
            // seat demand high watermark over the last period and its
            // configured limit, BUT not lower than the lower bound from
            // configuration. See KEP-1040.
            let min_current_cl = l.min_cl.max(l.nominal_cl.min(b.stats.high_watermark));
            idx_of_non_exempt.insert(name, items.len());
            non_exempt_names.push(name);
            items.push(AllocProblemItem {
                lower_bound: min_current_cl as f64,
                upper_bound: l.max_cl as f64,
                target: (min_current_cl as f64).max(b.stats.smoothed),
            });
            min_cl_sum += l.min_cl;
            min_current_cl_sum += min_current_cl;
        }
    }
    if items.is_empty() && nominal_cl_sum > 0 {
        error!("Impossible: no priority levels");
        return;
    }
    let mut allocs: Vec<f64> = Vec::new();
    let mut share_frac = 0f64;
    let mut backstop = false;
    // `metrics.SetFairFrac` (apf_controller.go:440-454).
    if remaining_server_cl <= min_cl_sum as i64 {
        // every non-exempt level gets its minCL
        fcmetrics::set_fair_frac(0.0);
    } else if remaining_server_cl <= min_current_cl_sum as i64 {
        share_frac = (remaining_server_cl - min_cl_sum as i64) as f64
            / (min_current_cl_sum - min_cl_sum) as f64;
        fcmetrics::set_fair_frac(0.0);
    } else {
        match compute_concurrency_allocation(nominal_cl_sum as i64, &items) {
            Ok((a, fair_frac)) => {
                allocs = a;
                fcmetrics::set_fair_frac(fair_frac);
            }
            Err(e) => {
                error!(
                    "Unable to derive new concurrency limits for {:?}: {}",
                    non_exempt_names, e
                );
                backstop = true;
                allocs = non_exempt_names
                    .iter()
                    .map(|n| levels[*n].borrow.lock().unwrap().current_cl as f64)
                    .collect();
            }
        }
    }
    for (name, l) in levels {
        let mut b = l.borrow.lock().unwrap();
        let current_cl = match idx_of_non_exempt.get(name.as_str()) {
            None => ccl_of_exempt[name.as_str()],
            Some(_) if remaining_server_cl <= min_cl_sum as i64 => l.min_cl,
            Some(_) if remaining_server_cl <= min_current_cl_sum as i64 => {
                let min_current_cl = l.min_cl.max(l.nominal_cl.min(b.stats.high_watermark));
                l.min_cl + ((min_current_cl - l.min_cl) as f64 * share_frac).round() as usize
            }
            Some(&idx) => allocs[idx].round() as usize,
        };
        let rel_change = rel_diff(current_cl as f64, b.current_cl as f64);
        b.current_cl = current_cl;
        // `NotePriorityLevelConcurrencyAdjustment` (apf_controller.go:481).
        // `items[idx].target` with `idx` the zero value for an exempt level
        // (not in `idxOfNonExempt`) reads the first item, as upstream does.
        let target = idx_of_non_exempt
            .get(name.as_str())
            .or(Some(&0))
            .and_then(|&i| items.get(i))
            .map_or(0.0, |it| it.target);
        fcmetrics::note_priority_level_concurrency_adjustment(
            name,
            b.stats.high_watermark as f64,
            b.stats.avg,
            b.stats.std_dev,
            b.stats.smoothed,
            target,
            current_cl as i64,
        );
        if rel_change >= 0.05 {
            tracing::info!(
                pl = %name, current_cl, high_watermark = b.stats.high_watermark,
                avg = b.stats.avg, std_dev = b.stats.std_dev, smoothed = b.stats.smoothed,
                backstop, "Update CurrentCL"
            );
        }
        let qcfg = queuing_config_for_pl(&l.pl);
        // `queueset.go:276-284`: the gauge denominators; the concurrency
        // denominator is `currentCL`, else `max(1, round(serverCL/10))`
        // (apf_controller.go:486-491).
        let concurrency_denominator = if current_cl > 0 {
            current_cl as f64
        } else {
            (server_cl as f64 / 10.0).round().max(1.0)
        };
        fcmetrics::set_level_denominators(
            name,
            queue_capacity(&qcfg) as f64,
            concurrency_denominator,
        );
        l.queues
            .set_configuration(
                qcfg,
                DispatchingConfig {
                    concurrency_limit: current_cl,
                },
            )
            .expect("existing priority level's queueing config was validated");
    }
}

/// `nominalCL`, `minCL`, `maxCL` and `currentCL` of a priority level.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BorrowingState {
    pub nominal_cl: usize,
    pub min_cl: usize,
    pub max_cl: usize,
    pub current_cl: usize,
}

/// Errors from [`FlowControlEngine::execute`].
#[derive(Debug, PartialEq, Eq)]
pub enum FlowControlError {
    /// Respond 429 with `Retry-After: 1` (priority-and-fairness.go:377-381).
    TooManyRequests,
}

impl std::fmt::Display for FlowControlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Too many requests, please try again later.")
    }
}

impl std::error::Error for FlowControlError {}

/// Holds seats while a request executes; released on drop.
pub struct FlowControlPermit {
    _execution: Option<Execution>,
    /// The metrics `Handle`'s dispatched closure maintains (apf_filter.go:181-193);
    /// dropped (and so recorded) with the permit, i.e. when the seats are
    /// released.
    metrics: Option<ExecutionMetrics>,
}

impl FlowControlPermit {
    /// A permit with no seat behind it (exempt/unknown level).
    fn untracked() -> Self {
        Self {
            _execution: None,
            metrics: None,
        }
    }

    /// Label the execution `type="watch"` for `request_execution_seconds`
    /// (`ObserveExecutionDuration`, metrics.go:585-592: `RequestInfo.Verb == "watch"`).
    pub fn mark_watch(&mut self) {
        if let Some(m) = self.metrics.as_mut() {
            m.is_watch = true;
        }
    }
}

/// Seats a request holds in a queue until it dispatches or gives up
/// (`AddRequestsInQueues`/`AddSeatsInQueues`, queueset.go:646, :705, :434).
struct InQueue {
    priority_level: String,
    flow_schema: String,
    seats: i64,
    mutating: bool,
}

impl InQueue {
    fn new(priority_level: &str, flow_schema: &str, seats: usize, mutating: bool) -> Self {
        fcmetrics::add_in_queues(priority_level, flow_schema, 1, seats as i64);
        // `reqsGaugePair.RequestsWaiting.Add(1)` (queueset.go:649) and
        // `noteWaitingDelta(1)` (priority-and-fairness.go:150-156).
        fcmetrics::add_level_waiting(priority_level, 1.0);
        fcmetrics::add_read_write(fcmetrics::Phase::Waiting, mutating, 1);
        Self {
            priority_level: priority_level.to_string(),
            flow_schema: flow_schema.to_string(),
            seats: seats as i64,
            mutating,
        }
    }
}

impl Drop for InQueue {
    fn drop(&mut self) {
        fcmetrics::add_in_queues(&self.priority_level, &self.flow_schema, -1, -self.seats);
        fcmetrics::add_level_waiting(&self.priority_level, -1.0);
        fcmetrics::add_read_write(fcmetrics::Phase::Waiting, self.mutating, -1);
    }
}

/// A dispatched request's executing gauges (`AddRequestsExecuting` +
/// `AddSeatConcurrencyInUse`, queueset.go:678, :724, :866, :878) and its
/// execution duration (apf_filter.go:187-191).
struct ExecutionMetrics {
    priority_level: String,
    flow_schema: String,
    seats: i64,
    started: Instant,
    is_watch: bool,
    mutating: bool,
}

impl ExecutionMetrics {
    fn new(priority_level: &str, flow_schema: &str, seats: usize, mutating: bool) -> Self {
        fcmetrics::add_executing(priority_level, flow_schema, 1, seats as i64);
        // `RequestsExecuting.Add(1)` + `execSeatsGauge.Add(seats)`
        // (queueset.go:680-681, :726-727) and `noteExecutingDelta(1)`.
        fcmetrics::add_level_executing(priority_level, 1.0, seats as f64);
        fcmetrics::add_read_write(fcmetrics::Phase::Executing, mutating, 1);
        Self {
            priority_level: priority_level.to_string(),
            flow_schema: flow_schema.to_string(),
            seats: seats as i64,
            started: Instant::now(),
            is_watch: false,
            mutating,
        }
    }
}

impl Drop for ExecutionMetrics {
    fn drop(&mut self) {
        fcmetrics::add_executing(&self.priority_level, &self.flow_schema, -1, -self.seats);
        fcmetrics::add_level_executing(&self.priority_level, -1.0, -(self.seats as f64));
        fcmetrics::add_read_write(fcmetrics::Phase::Executing, self.mutating, -1);
        fcmetrics::observe_execution_duration(
            &self.priority_level,
            &self.flow_schema,
            self.is_watch,
            self.started.elapsed(),
        );
    }
}

pub struct FlowControlEngine<S: Storage> {
    storage: Arc<S>,
    server_cl: i64,
    /// The clock queuesets and seat-demand integrators read.
    clock: Arc<dyn Clock>,
    config: RwLock<Arc<Config>>,
}

impl<S: Storage> FlowControlEngine<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self::with_limits(
            storage,
            DEFAULT_MAX_REQUESTS_IN_FLIGHT,
            DEFAULT_MAX_MUTATING_REQUESTS_IN_FLIGHT,
        )
    }

    /// [`Self::with_limits`] with the clock the queuesets and seat-demand
    /// integrators read (upstream: `TestableConfig.Clock`).
    pub fn with_limits_and_clock(
        storage: Arc<S>,
        max_inflight: i64,
        max_mutating_inflight: i64,
        clock: Arc<dyn Clock>,
    ) -> Self {
        let server_cl = max_inflight + max_mutating_inflight;
        let config = RwLock::new(Arc::new(digest(vec![], vec![], server_cl, None, &clock)));
        Self {
            storage,
            server_cl,
            clock,
            config,
        }
    }

    pub fn with_limits(storage: Arc<S>, max_inflight: i64, max_mutating_inflight: i64) -> Self {
        Self::with_limits_and_clock(
            storage,
            max_inflight,
            max_mutating_inflight,
            Arc::new(RealClock::default()),
        )
    }

    /// (Re)load FlowSchemas and PriorityLevelConfigurations from storage.
    pub async fn initialize(&self) -> Result<(), Box<dyn std::error::Error>> {
        let fss: Vec<FlowSchema> = self
            .storage
            .list(&build_key("flowschemas", None, ""))
            .await
            .unwrap_or_else(|e| {
                warn!("Failed to load FlowSchemas: {}", e);
                Vec::new()
            });
        let pls: Vec<PriorityLevelConfiguration> = self
            .storage
            .list(&build_key("prioritylevelconfigurations", None, ""))
            .await
            .unwrap_or_else(|e| {
                warn!("Failed to load PriorityLevelConfigurations: {}", e);
                Vec::new()
            });
        // Digest under the write lock (`lockAndDigestConfigObjects`) so that
        // "idle" cannot change between the check and the swap.
        self.digest_config_objects(pls, fss);
        Ok(())
    }

    /// The storage the configuration is read from.
    pub fn storage(&self) -> Arc<S> {
        self.storage.clone()
    }

    /// `syncOne` (apf_controller.go:535-546): list every FlowSchema and
    /// PriorityLevelConfiguration and digest them together. Unlike
    /// [`Self::initialize`] a failed list is an error, so that the caller
    /// requeues rate-limited (`processNextWorkItem`, :517-520) instead of
    /// digesting an empty configuration.
    pub async fn sync_one(&self) -> Result<(), Box<dyn std::error::Error>> {
        let pls: Vec<PriorityLevelConfiguration> = self
            .storage
            .list(&build_key("prioritylevelconfigurations", None, ""))
            .await
            .map_err(|e| format!("unable to list PriorityLevelConfiguration objects: {e}"))?;
        let fss: Vec<FlowSchema> = self
            .storage
            .list(&build_key("flowschemas", None, ""))
            .await
            .map_err(|e| format!("unable to list FlowSchema objects: {e}"))?;
        self.digest_config_objects(pls, fss);
        Ok(())
    }

    /// First matching FlowSchema in precedence order; `catch-all` is always
    /// present and matches everything.
    pub fn classify(&self, d: &RequestDigest) -> Classification {
        let cfg = self.config.read().unwrap().clone();
        let fs = cfg
            .flow_schemas
            .iter()
            .find(|f| matches_flow_schema(d, f))
            .unwrap_or_else(|| cfg.flow_schemas.last().expect("catch-all present"));
        let flow_distinguisher = match fs.spec.distinguisher_method.as_ref().map(|m| &m.type_) {
            Some(FlowDistinguisherMethodType::ByUser) => d.user_name.clone(),
            Some(FlowDistinguisherMethodType::ByNamespace) => d.namespace.clone(),
            _ => String::new(),
        };
        let priority_level = fs.spec.priority_level_configuration.name.clone();
        Classification {
            flow_schema: fs.metadata.name.clone(),
            flow_schema_uid: fs.metadata.uid.clone(),
            priority_level_uid: cfg
                .levels
                .get(&priority_level)
                .map(|l| l.pl.metadata.uid.clone())
                .unwrap_or_default(),
            priority_level,
            flow_distinguisher,
            is_mutating: !matches!(d.verb.as_str(), "get" | "list" | "watch"),
        }
    }

    /// `GetMaxSeats` (max_seats.go:34), set in `finishQueueSetReconfigsLocked`
    /// (apf_controller.go:882-894): MAX(1, MIN(ceil(0.15 * nominalCL),
    /// nominalCL / handSize)), only for levels that queue; 0 otherwise.
    pub fn max_seats(&self, priority_level: &str) -> u64 {
        let cfg = self.config.read().unwrap();
        let Some(l) = cfg.levels.get(priority_level) else {
            return 0;
        };
        let Some(qc) =
            l.pl.spec
                .limited
                .as_ref()
                .and_then(|l| l.limit_response.as_ref())
                .and_then(|lr| lr.queuing.as_ref())
        else {
            return 0;
        };
        let by_percent = (l.nominal_cl as f64 * PRIORITY_LEVEL_MAX_SEATS_PERCENT).ceil();
        let by_hand = if qc.hand_size > 0 {
            (l.nominal_cl as i64 / qc.hand_size as i64) as f64
        } else {
            by_percent
        };
        by_percent.min(by_hand).max(1.0) as u64
    }

    /// The queueset of a priority level (test and reaper support).
    pub fn queueset_for(&self, priority_level: &str) -> Option<Arc<QueueSet>> {
        self.config
            .read()
            .unwrap()
            .levels
            .get(priority_level)
            .map(|l| l.queues.clone())
    }

    /// Whether a priority level is undesired but still draining.
    pub fn is_quiescing(&self, priority_level: &str) -> bool {
        self.config
            .read()
            .unwrap()
            .levels
            .get(priority_level)
            .is_some_and(|l| l.quiescing)
    }

    /// `maybeReap` (apf_controller.go:1085-1100): once a quiescing level is
    /// idle, re-digest the current config so its last traces are removed
    /// (upstream enqueues `configQueue.Add(0)`; here it is done inline).
    /// Call it when a request of that level finishes with the queueset idle
    /// (apf_filter.go:171-177).
    pub fn maybe_reap(&self, priority_level: &str) {
        let useless = self
            .config
            .read()
            .unwrap()
            .levels
            .get(priority_level)
            .is_some_and(|l| l.quiescing && l.queues.is_idle());
        if !useless {
            return;
        }
        let mut cfg = self.config.write().unwrap();
        let (pls, fss) = cfg.inputs.clone();
        let next = digest(pls, fss, self.server_cl, Some(&cfg), &self.clock);
        *cfg = Arc::new(next);
    }

    /// Concurrency limit (seats) of a priority level, if known.
    pub fn concurrency_limit(&self, priority_level: &str) -> Option<usize> {
        self.config
            .read()
            .unwrap()
            .levels
            .get(priority_level)
            .map(|l| l.nominal_cl)
    }

    /// `lockAndDigestConfigObjects` (apf_controller.go:595).
    pub fn digest_config_objects(
        &self,
        pls: Vec<PriorityLevelConfiguration>,
        fss: Vec<FlowSchema>,
    ) {
        let mut cfg = self.config.write().unwrap();
        let next = digest(pls, fss, self.server_cl, Some(&cfg), &self.clock);
        *cfg = Arc::new(next);
    }

    /// `updateBorrowing` (apf_controller.go:393): run every
    /// [`BORROWING_ADJUSTMENT_PERIOD`] under the config write lock.
    pub fn update_borrowing(&self) {
        // The write lock only serializes with `digest` and with requests
        // starting (upstream: `cfgCtlr.lock.Lock()`); the `Config` itself is
        // mutated through its per-level mutexes.
        #[allow(clippy::readonly_write_lock)]
        let cfg = self.config.write().unwrap();
        update_borrowing_locked(&cfg.levels, cfg.nominal_cl_sum, self.server_cl);
    }

    /// The borrowing bounds and current limit of a level.
    pub fn borrowing_state(&self, priority_level: &str) -> Option<BorrowingState> {
        let cfg = self.config.read().unwrap();
        let l = cfg.levels.get(priority_level)?;
        let current_cl = l.borrow.lock().unwrap().current_cl;
        Some(BorrowingState {
            nominal_cl: l.nominal_cl,
            min_cl: l.min_cl,
            max_cl: l.max_cl,
            current_cl,
        })
    }

    /// The level's seat-demand integrator
    /// (`priorityLevelState.seatDemandIntegrator`).
    pub fn seat_demand(&self, priority_level: &str) -> Option<Arc<Integrator>> {
        let cfg = self.config.read().unwrap();
        cfg.levels
            .get(priority_level)
            .map(|l| l.queues.seat_demand())
    }

    /// `startRequest` + `Request.Finish` (apf_controller.go:1022-1079,
    /// apf_filter.go:155): start the request in its level's queueset and wait
    /// at most `wait_limit` (upstream: `newReqWaitCtxFn`) to be dispatched.
    /// Dropping the returned permit finishes the request.
    pub async fn execute(
        &self,
        c: &Classification,
        seats: u32,
        wait_limit: Duration,
    ) -> Result<FlowControlPermit, FlowControlError> {
        let we = WorkEstimate {
            initial_seats: seats.max(1) as u64,
            final_seats: 0,
            additional_latency: Duration::ZERO,
        };
        self.execute_with_estimate(c, &we, wait_limit).await
    }

    /// [`Self::execute`] with the work estimator's full `WorkEstimate`
    /// (`workEstimator()` result, priority-and-fairness.go:121-135).
    pub async fn execute_with_estimate(
        &self,
        c: &Classification,
        we: &WorkEstimate,
        wait_limit: Duration,
    ) -> Result<FlowControlPermit, FlowControlError> {
        // Hold the read lock across the synchronous `start_request`
        // (upstream: startRequest runs under `cfgCtlr.lock.RLock()`), so a
        // concurrent digest cannot judge the level idle and drop it between
        // our lookup and the request registering.
        let started = {
            let guard = self.config.read().unwrap();
            let Some(level) = guard.levels.get(&c.priority_level) else {
                return Ok(FlowControlPermit::untracked());
            };
            // The flow distinguisher and hash only matter with more than one
            // queue.
            let (hash_value, flow_distinguisher) = if !level.exempt && level.num_queues > 1 {
                (
                    hash_flow_id(&c.flow_schema, &c.flow_distinguisher),
                    c.flow_distinguisher.as_str(),
                )
            } else {
                (0, "")
            };
            level
                .queues
                .start_request(we, hash_value, flow_distinguisher, &c.flow_schema)
        };
        let (pl, fs) = (c.priority_level.as_str(), c.flow_schema.as_str());
        let handle = match started {
            Ok(h) => h,
            Err(rejected) => {
                // `AddReject` (queueset.go:321, :340).
                fcmetrics::add_reject(
                    pl,
                    fs,
                    match rejected.reason {
                        RejectReason::ConcurrencyLimit => "concurrency-limit",
                        RejectReason::QueueFull => "queue-full",
                    },
                );
                // `if idle { maybeReapReadLocked }` (apf_controller.go:1075-1077)
                if rejected.idle {
                    self.maybe_reap(&c.priority_level);
                }
                return Err(FlowControlError::TooManyRequests);
            }
        };
        let seats = we.max_seats();
        // `queued := startWaitingTime != time.Time{}` (apf_filter.go:158):
        // the request is waiting in a queue rather than already dispatched.
        let queued = if handle.is_dispatched() {
            None
        } else {
            Some((InQueue::new(pl, fs, seats, c.is_mutating), Instant::now()))
        };
        // Dropping the wait future on timeout cancels the queued request.
        match tokio::time::timeout(wait_limit, handle.wait()).await {
            Ok(exec) => {
                if let Some((in_queue, since)) = queued {
                    drop(in_queue);
                    // `observeQueueWaitTime(..., FormatBool(req != nil), ...)`
                    // (apf_filter.go:183-185).
                    fcmetrics::observe_waiting_duration(pl, fs, true, since.elapsed());
                }
                exec.note_dispatched();
                // `AddDispatch` (apf_filter.go:187).
                fcmetrics::add_dispatch(pl, fs);
                Ok(FlowControlPermit {
                    _execution: Some(exec),
                    metrics: Some(ExecutionMetrics::new(pl, fs, seats, c.is_mutating)),
                })
            }
            Err(_) => {
                if let Some((in_queue, since)) = queued {
                    drop(in_queue);
                    // `if queued && !executed` (apf_filter.go:199-201); `req`
                    // is non-nil here, so `execute` is "true" as upstream.
                    fcmetrics::observe_waiting_duration(pl, fs, true, since.elapsed());
                }
                // `AddReject(..., "time-out")` (queueset.go:433).
                fcmetrics::add_reject(pl, fs, "time-out");
                Err(FlowControlError::TooManyRequests)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_storage::memory::MemoryStorage;

    fn cls(fs: &str, pl: &str, dist: &str) -> Classification {
        Classification {
            flow_schema: fs.into(),
            priority_level: pl.into(),
            flow_distinguisher: dist.into(),
            ..Default::default()
        }
    }

    fn engine() -> FlowControlEngine<MemoryStorage> {
        FlowControlEngine::new(Arc::new(MemoryStorage::new()))
    }

    fn digest_for(user: &str, groups: &[&str], verb: &str, path: &str) -> RequestDigest {
        RequestDigest {
            user_name: user.into(),
            groups: groups.iter().map(|s| s.to_string()).collect(),
            verb: verb.into(),
            path: path.into(),
            ..Default::default()
        }
    }

    #[test]
    fn mandatory_exempt_matches_masters_group() {
        let e = engine();
        let mut d = digest_for("admin", &["system:masters"], "get", "/healthz");
        d.is_resource_request = true;
        d.resource = "pods".into();
        let c = e.classify(&d);
        assert_eq!(c.flow_schema, "exempt");
        assert_eq!(c.priority_level, "exempt");
    }

    #[test]
    fn everything_else_falls_to_catch_all() {
        let e = engine();
        let mut d = digest_for("bob", &["system:authenticated"], "list", "");
        d.is_resource_request = true;
        d.resource = "pods".into();
        d.namespace = "default".into();
        let c = e.classify(&d);
        assert_eq!(c.flow_schema, "catch-all");
        assert_eq!(c.priority_level, "catch-all");
    }

    #[test]
    fn catch_all_nominal_cl_is_whole_server_limit() {
        // exempt shares 0 (plSpecCommons), catch-all shares 5 => ceil(600*5/5).
        let e = engine();
        assert_eq!(e.concurrency_limit("catch-all"), Some(600));
        assert_eq!(e.concurrency_limit("exempt"), Some(0));
    }

    #[test]
    fn non_resource_url_prefix_matching() {
        let urls = vec!["/healthz/*".to_string()];
        assert!(matches_non_resource_url(&urls, "/healthz/ping"));
        assert!(!matches_non_resource_url(&urls, "/healthzz"));
        assert!(matches_non_resource_url(&["*".to_string()], "/anything"));
    }

    #[test]
    fn service_account_namespace_wildcard() {
        assert!(sa_matches_namespace(
            "kube-system",
            "system:serviceaccount:kube-system:x"
        ));
        assert!(!sa_matches_namespace(
            "kube",
            "system:serviceaccount:kube-system:x"
        ));
    }

    #[tokio::test]
    async fn exempt_never_waits() {
        let e = engine();
        assert!(e
            .execute(&cls("exempt", "exempt", ""), 1, Duration::from_millis(1))
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn limited_level_rejects_when_seats_exhausted() {
        // catch-all is `Reject` upstream: a request beyond the limit is 429.
        let e = FlowControlEngine::with_limits(Arc::new(MemoryStorage::new()), 1, 0);
        assert_eq!(e.concurrency_limit("catch-all"), Some(1));
        let held = e
            .execute(
                &cls("catch-all", "catch-all", ""),
                1,
                Duration::from_millis(5),
            )
            .await
            .unwrap();
        let second = e
            .execute(
                &cls("catch-all", "catch-all", ""),
                1,
                Duration::from_millis(5),
            )
            .await;
        assert_eq!(second.err(), Some(FlowControlError::TooManyRequests));
        drop(held);
        assert!(e
            .execute(
                &cls("catch-all", "catch-all", ""),
                1,
                Duration::from_millis(5)
            )
            .await
            .is_ok());
    }

    // ---- borrowing (#2743; exempt_borrowing_test.go) ----

    fn borrowing_pl(
        name: &str,
        shares: i32,
        lendable: Option<i32>,
        borrowing: Option<i32>,
    ) -> PriorityLevelConfiguration {
        let mut pl = queueing_pl(128, 6, 50);
        pl.metadata = ObjectMeta::new(name);
        let l = pl.spec.limited.as_mut().unwrap();
        l.nominal_concurrency_shares = Some(shares);
        l.lendable_percent = lendable;
        l.borrowing_limit_percent = borrowing;
        pl
    }

    /// `TestUpdateBorrowing` (exempt_borrowing_test.go:33). The levels mirror
    /// `SuggestedPriorityLevelConfigurationWorkloadHigh` (shares 40, lendable
    /// 50%), `...WorkloadLow` (100, 90%) and the mandatory catch-all (5, 0%).
    #[test]
    fn update_borrowing_matches_upstream_exempt_borrowing_test() {
        use crate::flow_control_integrator::test_clock::ManualClock;
        let period = BORROWING_ADJUSTMENT_PERIOD;
        let clk = ManualClock::new();
        let server_cl = (40 + 100 + 5) * 6;
        let e = FlowControlEngine::with_limits_and_clock(
            Arc::new(MemoryStorage::new()),
            server_cl,
            0,
            clk.clone(),
        );
        e.digest_config_objects(
            vec![
                borrowing_pl("high", 40, Some(50), None),
                mandatory_priority_level_configuration("exempt").unwrap(),
                borrowing_pl("mid", 100, Some(90), None),
                mandatory_priority_level_configuration("catch-all").unwrap(),
            ],
            vec![],
        );
        let st = |n: &str| e.borrowing_state(n).unwrap();
        let demand = |n: &str, x: f64| e.seat_demand(n).unwrap().set(x);
        let (high, mid, low) = (st("high"), st("mid"), st("catch-all"));
        assert_eq!(
            high.nominal_cl + mid.nominal_cl + low.nominal_cl,
            server_cl as usize
        );

        // Scenario 1: everybody wants more than ServerConcurrencyLimit.
        // Exempt borrows so much that less than minCL is left for each
        // non-exempt level.
        for n in ["exempt", "high", "mid", "catch-all"] {
            demand(n, (server_cl + 100) as f64);
        }
        clk.set(period);
        e.update_borrowing();
        assert_eq!(st("exempt").current_cl, server_cl as usize + 100);
        assert_eq!(st("high").current_cl, high.min_cl);
        assert_eq!(st("mid").current_cl, mid.min_cl);
        assert_eq!(st("catch-all").current_cl, low.min_cl);

        // Scenario 2: non-exempt want more than serverCL but get halfway
        // between minCL and minCurrentCL.
        let exp_high = (high.nominal_cl + high.min_cl) / 2;
        let exp_mid = (mid.nominal_cl + mid.min_cl) / 2;
        let exp_low = (low.nominal_cl + low.min_cl) / 2;
        let exp_exempt = server_cl as usize - (exp_high + exp_mid + exp_low);
        demand("exempt", exp_exempt as f64);
        clk.set(2 * period);
        e.update_borrowing();
        clk.set(3 * period);
        e.update_borrowing();
        assert_eq!(st("exempt").current_cl, exp_exempt);
        assert_eq!(st("high").current_cl, exp_high);
        assert_eq!(st("mid").current_cl, exp_mid);
        assert_eq!(st("catch-all").current_cl, exp_low);

        // Scenario 3: only mid is willing to lend, and exempt borrows all of
        // that (regular borrowing).
        let exp_high = high.nominal_cl;
        let exp_mid = mid.min_cl;
        let exp_low = low.nominal_cl;
        let exp_exempt = server_cl as usize - (exp_high + exp_mid + exp_low);
        demand("exempt", exp_exempt as f64);
        demand("mid", 1.0);
        clk.set(4 * period);
        e.update_borrowing();
        clk.set(5 * period);
        e.update_borrowing();
        assert_eq!(st("exempt").current_cl, exp_exempt);
        assert_eq!(st("high").current_cl, exp_high);
        assert_eq!(st("mid").current_cl, exp_mid);
        assert_eq!(st("catch-all").current_cl, exp_low);
    }

    /// `finishQueueSetReconfigsLocked` (apf_controller.go:859-900): min/max
    /// from lendable/borrowing percents, initial currentCL = nominal -
    /// lendable/2, and a level with no demand lends down to its minCL.
    #[test]
    fn digest_derives_min_max_and_initial_current_cl() {
        use crate::flow_control_integrator::test_clock::ManualClock;
        let e = FlowControlEngine::with_limits_and_clock(
            Arc::new(MemoryStorage::new()),
            100,
            0,
            ManualClock::new(),
        );
        e.digest_config_objects(
            vec![
                borrowing_pl("a", 50, Some(40), Some(30)),
                borrowing_pl("b", 50, None, Some(0)),
            ],
            vec![],
        );
        // catch-all (5 shares) joins: shareSum 105.
        let a = e.borrowing_state("a").unwrap();
        assert_eq!(a.nominal_cl, 48); // ceil(100*50/105)
        assert_eq!(a.min_cl, 48 - 19); // lendable round(48*0.4)=19
        assert_eq!(a.max_cl, 48 + 14); // borrowing round(48*0.3)=14
        let b = e.borrowing_state("b").unwrap();
        assert_eq!((b.min_cl, b.max_cl), (48, 48));
        assert_eq!(b.current_cl, 48);
        // No demand has been observed, so the first adjustment lends all it can.
        e.update_borrowing();
        assert!(e.borrowing_state("a").unwrap().current_cl >= a.min_cl);
    }

    /// A busy level borrows what an idle lendable level lends, and the
    /// queueset enforces the new `currentCL` (the point of
    /// `updateBorrowingLocked` completing the queueset with it).
    #[test]
    fn busy_level_borrows_from_idle_lendable_level() {
        use crate::flow_control_integrator::test_clock::ManualClock;
        let clk = ManualClock::new();
        let e = FlowControlEngine::with_limits_and_clock(
            Arc::new(MemoryStorage::new()),
            100,
            0,
            clk.clone(),
        );
        e.digest_config_objects(
            vec![
                borrowing_pl("busy", 50, Some(50), None),
                borrowing_pl("idle", 50, Some(50), None),
            ],
            vec![],
        );
        let busy = e.borrowing_state("busy").unwrap();
        assert!(busy.current_cl < busy.nominal_cl + 1);
        e.seat_demand("busy").unwrap().set(1000.0);
        clk.set(BORROWING_ADJUSTMENT_PERIOD);
        e.update_borrowing();
        let after = e.borrowing_state("busy").unwrap();
        assert!(
            after.current_cl > after.nominal_cl,
            "expected to borrow above nominal {}, got {}",
            after.nominal_cl,
            after.current_cl
        );
        assert!(after.current_cl <= after.max_cl);
        assert_eq!(
            e.queueset_for("busy").unwrap().concurrency_limit(),
            after.current_cl
        );
        let idle = e.borrowing_state("idle").unwrap();
        assert!(idle.current_cl >= idle.min_cl && idle.current_cl < idle.nominal_cl);
    }

    // ---- QueueSet wiring (#2741) ----

    use rusternetes_common::resources::flowcontrol::{
        LimitResponse, LimitResponseType, LimitedPriorityLevelConfiguration, QueuingConfiguration,
    };
    use rusternetes_common::types::ObjectMeta;

    /// A `Limited`/`Queue` level named `test` (CL 1 when the server limit is 1).
    fn queueing_pl(queues: i32, hand: i32, len: i32) -> PriorityLevelConfiguration {
        let mut pl = mandatory_priority_level_configuration("catch-all").unwrap();
        pl.metadata = ObjectMeta::new("test");
        pl.spec.limited = Some(LimitedPriorityLevelConfiguration {
            nominal_concurrency_shares: Some(1000),
            lending_concurrency_limit: None,
            lendable_percent: None,
            borrowing_limit_percent: None,
            limit_response: Some(LimitResponse {
                type_: LimitResponseType::Queue,
                queuing: Some(QueuingConfiguration {
                    queues,
                    hand_size: hand,
                    queue_length_limit: len,
                }),
            }),
        });
        pl
    }

    fn reject_pl() -> PriorityLevelConfiguration {
        let mut pl = queueing_pl(0, 0, 0);
        if let Some(l) = pl.spec.limited.as_mut() {
            l.limit_response = Some(LimitResponse {
                type_: LimitResponseType::Reject,
                queuing: None,
            });
        }
        pl
    }

    async fn engine_with(pl: PriorityLevelConfiguration) -> FlowControlEngine<MemoryStorage> {
        let st = Arc::new(MemoryStorage::new());
        st.create(&build_key("prioritylevelconfigurations", None, "test"), &pl)
            .await
            .unwrap();
        let e = FlowControlEngine::with_limits(st, 1, 0);
        e.initialize().await.unwrap();
        e
    }

    #[test]
    fn hash_flow_id_matches_upstream_vectors() {
        // sha256(fsName || 0x00 || distinguisher)[:8] little-endian
        // (apf_controller.go:1131-1140); vectors from python hashlib.
        assert_eq!(hash_flow_id("fs", "a"), 3672294129925421940);
        assert_eq!(hash_flow_id("fs", "b"), 11054805014913603293);
        assert_eq!(hash_flow_id("", ""), 10987292151339758702);
        assert_eq!(hash_flow_id("catch-all", "user"), 2294043605982591715);
    }

    #[tokio::test]
    async fn queued_requests_are_dispatched_fairly_across_flows() {
        // One seat; flow a queues 3 requests, then flow b queues 1. Fair
        // queueing serves b before a's second request; a FIFO semaphore does not.
        let e = Arc::new(engine_with(queueing_pl(64, 1, 10)).await);
        let held = e
            .execute(&cls("fs", "test", "h"), 1, Duration::from_secs(5))
            .await
            .unwrap();
        let order = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let mut tasks = vec![];
        for (name, flow) in [("a1", "a"), ("a2", "a"), ("a3", "a"), ("b1", "b")] {
            let (e, order) = (e.clone(), order.clone());
            tasks.push(tokio::spawn(async move {
                let p = e
                    .execute(&cls("fs", "test", flow), 1, Duration::from_secs(5))
                    .await
                    .unwrap();
                order.lock().unwrap().push(name.to_string());
                tokio::time::sleep(Duration::from_millis(10)).await;
                drop(p);
            }));
            tokio::task::yield_now().await;
        }
        drop(held);
        for t in tasks {
            t.await.unwrap();
        }
        let order = order.lock().unwrap().clone();
        let pos = |n: &str| order.iter().position(|x| x == n).unwrap();
        assert!(pos("b1") < pos("a2"), "unfair order: {order:?}");
    }

    #[tokio::test]
    async fn reload_keeps_in_flight_seats_accounted() {
        // Reconfigure via set_configuration (not a fresh queueset): a seat held
        // across `initialize()` must still count against the level.
        let e = engine_with(queueing_pl(4, 1, 1)).await;
        let held = e
            .execute(&cls("fs", "test", "a"), 1, Duration::from_millis(5))
            .await
            .unwrap();
        e.initialize().await.unwrap();
        let second = e
            .execute(&cls("fs", "test", "a"), 1, Duration::from_millis(20))
            .await;
        assert_eq!(second.err(), Some(FlowControlError::TooManyRequests));
        drop(held);
        assert!(e
            .execute(&cls("fs", "test", "a"), 1, Duration::from_millis(20))
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn queue_length_limit_rejects_immediately() {
        let e = Arc::new(engine_with(queueing_pl(1, 1, 1)).await);
        let _held = e
            .execute(&cls("fs", "test", "a"), 1, Duration::from_millis(5))
            .await
            .unwrap();
        let e2 = e.clone();
        let queued = tokio::spawn(async move {
            e2.execute(&cls("fs", "test", "a"), 1, Duration::from_millis(300))
                .await
        });
        tokio::time::sleep(Duration::from_millis(30)).await;
        let start = std::time::Instant::now();
        let third = e
            .execute(&cls("fs", "test", "a"), 1, Duration::from_secs(5))
            .await;
        assert_eq!(third.err(), Some(FlowControlError::TooManyRequests));
        assert!(start.elapsed() < Duration::from_millis(200));
        let _ = queued.await;
    }

    #[tokio::test]
    async fn exempt_is_not_limited_by_concurrency() {
        // Exempt: DesiredNumQueues < 0 dispatches immediately whatever the CL.
        let e = engine();
        let mut held = vec![];
        for _ in 0..1000 {
            held.push(
                e.execute(&cls("exempt", "exempt", ""), 5, Duration::from_millis(1))
                    .await
                    .unwrap(),
            );
        }
    }

    // ---- quiescing / reaping (#2769) ----
    //
    // Port of the invariant `TestConfigConsumer` checks after each digest
    // (controller_test.go:253-285): a queueset exists for every desired level,
    // every mandatory level, and every undesired level that is still busy; no
    // other queueset is non-idle.

    async fn put_pl(st: &Arc<MemoryStorage>, pl: &PriorityLevelConfiguration) {
        let key = build_key("prioritylevelconfigurations", None, &pl.metadata.name);
        if st.create(&key, pl).await.is_err() {
            st.update(&key, pl).await.unwrap();
        }
    }

    async fn del_pl(st: &Arc<MemoryStorage>, name: &str) {
        let _ = st
            .delete(&build_key("prioritylevelconfigurations", None, name))
            .await;
    }

    fn named_pl(name: &str, queues: i32, hand: i32) -> PriorityLevelConfiguration {
        let mut pl = queueing_pl(queues, hand, 10);
        pl.metadata = ObjectMeta::new(name);
        pl
    }

    async fn level_a_engine() -> (Arc<MemoryStorage>, FlowControlEngine<MemoryStorage>) {
        let st = Arc::new(MemoryStorage::new());
        put_pl(&st, &named_pl("a", 4, 2)).await;
        let e = FlowControlEngine::with_limits(st.clone(), 100, 0);
        e.initialize().await.unwrap();
        (st, e)
    }

    async fn hold_seat(e: &FlowControlEngine<MemoryStorage>, pl: &str) -> FlowControlPermit {
        e.execute(&cls("x", pl, "d"), 1, Duration::from_millis(50))
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn removed_idle_level_is_dropped() {
        let (st, e) = level_a_engine().await;
        assert!(e.queueset_for("a").is_some());
        del_pl(&st, "a").await;
        e.initialize().await.unwrap();
        assert!(e.queueset_for("a").is_none());
    }

    #[tokio::test]
    async fn removed_busy_level_quiesces_then_is_reaped() {
        // processOldPLsLocked (apf_controller.go:810-836) + maybeReap.
        let (st, e) = level_a_engine().await;
        let qs = e.queueset_for("a").unwrap();
        let permit = hold_seat(&e, "a").await;
        del_pl(&st, "a").await;
        e.initialize().await.unwrap();
        assert!(e.is_quiescing("a"));
        assert!(Arc::ptr_eq(&qs, &e.queueset_for("a").unwrap()));
        // Still busy: reaping does nothing.
        e.maybe_reap("a");
        assert!(e.queueset_for("a").is_some());
        drop(permit);
        e.maybe_reap("a");
        assert!(e.queueset_for("a").is_none());
    }

    #[tokio::test]
    async fn quiescing_level_that_becomes_desired_again_keeps_its_queueset() {
        // digestNewPLsLocked (apf_controller.go:737-740).
        let (st, e) = level_a_engine().await;
        let qs = e.queueset_for("a").unwrap();
        let permit = hold_seat(&e, "a").await;
        let pl = named_pl("a", 4, 2);
        del_pl(&st, "a").await;
        e.initialize().await.unwrap();
        assert!(e.is_quiescing("a"));
        put_pl(&st, &pl).await;
        e.initialize().await.unwrap();
        assert!(!e.is_quiescing("a"));
        assert!(Arc::ptr_eq(&qs, &e.queueset_for("a").unwrap()));
        drop(permit);
    }

    #[tokio::test]
    async fn broken_update_to_busy_level_keeps_old_queueset_quiescing() {
        // digestNewPLsLocked ignores a broken spec (:765-769); the old state
        // is then processed as an undesired level (:810-836).
        let (st, e) = level_a_engine().await;
        let qs = e.queueset_for("a").unwrap();
        let permit = hold_seat(&e, "a").await;
        // hand size larger than the deck: invalid shuffle sharding.
        put_pl(&st, &named_pl("a", 2, 5)).await;
        e.initialize().await.unwrap();
        let kept = e.queueset_for("a").expect("busy level must be retained");
        assert!(Arc::ptr_eq(&qs, &kept));
        assert!(e.is_quiescing("a"));
        drop(permit);
    }

    #[tokio::test]
    async fn broken_update_to_idle_level_drops_it() {
        let (st, e) = level_a_engine().await;
        put_pl(&st, &named_pl("a", 2, 5)).await;
        e.initialize().await.unwrap();
        assert!(e.queueset_for("a").is_none());
    }

    #[tokio::test]
    async fn broken_new_level_is_ignored() {
        let (st, e) = level_a_engine().await;
        put_pl(&st, &named_pl("b", 2, 5)).await;
        e.initialize().await.unwrap();
        assert!(e.queueset_for("b").is_none());
        assert!(e.queueset_for("a").is_some());
    }

    #[tokio::test]
    async fn mandatory_levels_are_never_dropped() {
        let (_st, e) = level_a_engine().await;
        for _ in 0..2 {
            e.initialize().await.unwrap();
            assert!(e.queueset_for("exempt").is_some());
            assert!(e.queueset_for("catch-all").is_some());
        }
    }

    #[tokio::test]
    async fn quiescing_level_still_gets_a_share_while_draining() {
        // processOldPLsLocked :852-857: lingering levels stay in shareSum.
        let (st, e) = level_a_engine().await;
        let before = e.concurrency_limit("a").unwrap();
        let permit = hold_seat(&e, "a").await;
        del_pl(&st, "a").await;
        e.initialize().await.unwrap();
        assert_eq!(e.concurrency_limit("a"), Some(before));
        drop(permit);
    }

    #[tokio::test]
    async fn digest_invariant_over_a_sequence_of_configs() {
        // Deterministic stand-in for TestConfigConsumer's random walk.
        let st = Arc::new(MemoryStorage::new());
        let e = FlowControlEngine::with_limits(st.clone(), 100, 0);
        let names = ["p0", "p1", "p2", "p3"];
        let mut held: Vec<(String, FlowControlPermit)> = vec![];
        let mut seed = 0x9E37_79B9_7F4A_7C15u64;
        let mut next = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let mut persisting: std::collections::HashSet<String> = Default::default();
        for _step in 0..40 {
            let mut desired = std::collections::HashSet::new();
            for n in names {
                match next() % 3 {
                    0 => del_pl(&st, n).await,
                    1 => {
                        put_pl(&st, &named_pl(n, 4, 2)).await;
                        desired.insert(n.to_string());
                    }
                    // broken: ignored, like an absent object
                    _ => put_pl(&st, &named_pl(n, 2, 5)).await,
                }
            }
            e.initialize().await.unwrap();
            let mut next_persist = std::collections::HashSet::new();
            for n in &persisting {
                if held.iter().any(|(h, _)| h == n) {
                    next_persist.insert(n.clone());
                }
            }
            persisting = next_persist.union(&desired).cloned().collect();
            for n in &persisting {
                assert!(e.queueset_for(n).is_some(), "missing queueset {n}");
            }
            for n in names {
                if !persisting.contains(n) {
                    assert!(
                        e.queueset_for(n).is_none_or(|q| q.is_idle()),
                        "unexpected busy queueset {n}"
                    );
                }
            }
            for n in &desired {
                if next() % 2 == 0 {
                    held.push((n.clone(), hold_seat(&e, n).await));
                }
            }
            if next() % 3 == 0 {
                held.clear();
            }
        }
    }

    // ---- apiserver_flowcontrol_* metrics (#2809) ----
    //
    // Ports of the observations `queueset.go` (AddReject :321/:340/:433,
    // AddRequestsInQueues :646/:705, AddRequestsExecuting :678/:724/:866,
    // AddSeatConcurrencyInUse) and `apf_filter.go` `Handle` (:164-201:
    // AddDispatch, ObserveExecutionDuration, observeQueueWaitTime) make.

    use super::metric_test_util::sample;

    #[tokio::test]
    async fn dispatch_counts_and_executing_gauges_follow_the_permit() {
        let e = engine_with(queueing_pl(4, 1, 10)).await;
        let l = [("priority_level", "test"), ("flow_schema", "m-dispatch")];
        let before = sample("apiserver_flowcontrol_dispatched_requests_total", &l);
        let permit = e
            .execute(
                &cls("m-dispatch", "test", "a"),
                1,
                Duration::from_millis(50),
            )
            .await
            .unwrap();
        assert_eq!(
            sample("apiserver_flowcontrol_dispatched_requests_total", &l) - before,
            1.0
        );
        assert_eq!(
            sample("apiserver_flowcontrol_current_executing_requests", &l),
            1.0
        );
        assert_eq!(
            sample("apiserver_flowcontrol_current_executing_seats", &l),
            1.0
        );
        let exec_before = sample("apiserver_flowcontrol_request_execution_seconds", &l);
        drop(permit);
        assert_eq!(
            sample("apiserver_flowcontrol_current_executing_requests", &l),
            0.0
        );
        assert_eq!(
            sample("apiserver_flowcontrol_current_executing_seats", &l),
            0.0
        );
        assert_eq!(
            sample("apiserver_flowcontrol_request_execution_seconds", &l) - exec_before,
            1.0
        );
    }

    #[tokio::test]
    async fn queue_full_is_counted_as_a_rejection() {
        let e = Arc::new(engine_with(queueing_pl(1, 1, 1)).await);
        let _held = e
            .execute(&cls("m-full", "test", "a"), 1, Duration::from_millis(5))
            .await
            .unwrap();
        let e2 = e.clone();
        let queued = tokio::spawn(async move {
            e2.execute(&cls("m-full", "test", "a"), 1, Duration::from_millis(300))
                .await
        });
        tokio::time::sleep(Duration::from_millis(30)).await;
        let l = [
            ("priority_level", "test"),
            ("flow_schema", "m-full"),
            ("reason", "queue-full"),
        ];
        let before = sample("apiserver_flowcontrol_rejected_requests_total", &l);
        let inq = [("priority_level", "test"), ("flow_schema", "m-full")];
        assert_eq!(
            sample("apiserver_flowcontrol_current_inqueue_requests", &inq),
            1.0
        );
        assert_eq!(
            sample("apiserver_flowcontrol_current_inqueue_seats", &inq),
            1.0
        );
        assert!(e
            .execute(&cls("m-full", "test", "a"), 1, Duration::from_millis(5))
            .await
            .is_err());
        assert_eq!(
            sample("apiserver_flowcontrol_rejected_requests_total", &l) - before,
            1.0
        );
        queued.abort();
    }

    #[tokio::test]
    async fn a_wait_that_times_out_is_a_time_out_rejection_and_leaves_the_queue() {
        let e = engine_with(queueing_pl(4, 1, 10)).await;
        let _held = e
            .execute(&cls("m-to", "test", "a"), 1, Duration::from_millis(5))
            .await
            .unwrap();
        let l = [
            ("priority_level", "test"),
            ("flow_schema", "m-to"),
            ("reason", "time-out"),
        ];
        let before = sample("apiserver_flowcontrol_rejected_requests_total", &l);
        assert!(e
            .execute(&cls("m-to", "test", "a"), 1, Duration::from_millis(20))
            .await
            .is_err());
        assert_eq!(
            sample("apiserver_flowcontrol_rejected_requests_total", &l) - before,
            1.0
        );
        let q = [("priority_level", "test"), ("flow_schema", "m-to")];
        assert_eq!(
            sample("apiserver_flowcontrol_current_inqueue_requests", &q),
            0.0
        );
        // The waited request is observed with execute="true" (`req != nil`,
        // apf_filter.go:201 call sites).
        assert_eq!(
            sample("apiserver_flowcontrol_request_wait_duration_seconds", &q),
            1.0
        );
    }

    #[tokio::test]
    async fn a_queued_request_that_dispatches_observes_its_wait() {
        let e = Arc::new(engine_with(queueing_pl(4, 1, 10)).await);
        let held = e
            .execute(&cls("m-wait", "test", "a"), 1, Duration::from_millis(5))
            .await
            .unwrap();
        let e2 = e.clone();
        let t = tokio::spawn(async move {
            e2.execute(&cls("m-wait", "test", "a"), 1, Duration::from_secs(5))
                .await
        });
        tokio::time::sleep(Duration::from_millis(30)).await;
        drop(held);
        let _p = t.await.unwrap().unwrap();
        let l = [
            ("priority_level", "test"),
            ("flow_schema", "m-wait"),
            ("execute", "true"),
        ];
        // Only the queued request observes a wait (`if queued`).
        assert_eq!(
            sample("apiserver_flowcontrol_request_wait_duration_seconds", &l),
            1.0
        );
    }

    #[tokio::test]
    async fn concurrency_limit_rejection_without_queues() {
        // desired_num_queues < 1 (Reject-type level): queueset.go:321.
        let e = engine_with(reject_pl()).await;
        let _held = e
            .execute(&cls("m-cl", "test", ""), 1, Duration::from_millis(5))
            .await
            .unwrap();
        let l = [
            ("priority_level", "test"),
            ("flow_schema", "m-cl"),
            ("reason", "concurrency-limit"),
        ];
        let before = sample("apiserver_flowcontrol_rejected_requests_total", &l);
        assert!(e
            .execute(&cls("m-cl", "test", ""), 1, Duration::from_millis(5))
            .await
            .is_err());
        assert_eq!(
            sample("apiserver_flowcontrol_rejected_requests_total", &l) - before,
            1.0
        );
    }

    // ---- #2960: limit gauges, utilization and read-vs-write histograms ----

    use super::metric_test_util::{has_series, sample_sum};

    const NS: &str = "apiserver_flowcontrol_";

    async fn engine_with_level(
        name: &str,
    ) -> (Arc<MemoryStorage>, FlowControlEngine<MemoryStorage>) {
        let st = Arc::new(MemoryStorage::new());
        put_pl(&st, &named_pl(name, 4, 2)).await;
        let e = FlowControlEngine::with_limits(st.clone(), 100, 0);
        e.initialize().await.unwrap();
        (st, e)
    }

    async fn hold(
        e: &FlowControlEngine<MemoryStorage>,
        pl: &str,
        mutating: bool,
    ) -> FlowControlPermit {
        let mut c = cls("x", pl, "d");
        c.is_mutating = mutating;
        e.execute(&c, 1, Duration::from_millis(50)).await.unwrap()
    }

    // SetPriorityLevelConfiguration (apf_controller.go:874),
    // NotePriorityLevelConcurrencyAdjustment (:481), SetFairFrac (:440-454).
    #[tokio::test]
    async fn digest_publishes_the_limit_gauges() {
        let (_st, e) = engine_with_level("m-limits").await;
        let (nominal, min, max, current) = {
            let cfg = e.config.read().unwrap();
            let l = &cfg.levels["m-limits"];
            let current_cl = l.borrow.lock().unwrap().current_cl;
            (l.nominal_cl, l.min_cl, l.max_cl, current_cl)
        };
        let l = [("priority_level", "m-limits")];
        assert!(nominal > 0);
        for name in ["nominal_limit_seats", "request_concurrency_limit"] {
            assert_eq!(sample(&format!("{NS}{name}"), &l), nominal as f64, "{name}");
        }
        assert_eq!(sample(&format!("{NS}lower_limit_seats"), &l), min as f64);
        assert_eq!(sample(&format!("{NS}upper_limit_seats"), &l), max as f64);
        assert_eq!(
            sample(&format!("{NS}current_limit_seats"), &l),
            current as f64
        );
        for name in [
            "demand_seats_high_watermark",
            "demand_seats_average",
            "demand_seats_stdev",
            "demand_seats_smoothed",
            "target_seats",
        ] {
            assert!(has_series(&format!("{NS}{name}"), &l), "{name} missing");
        }
        assert!(has_series(&format!("{NS}seat_fair_frac"), &[]));
    }

    // execSeatsGauge / reqsGaugePair (queueset.go:281-284, :680-681, :867-879):
    // the level's utilization histograms integrate over time.
    #[tokio::test]
    async fn utilization_histograms_integrate_a_held_seat() {
        let (_st, e) = engine_with_level("m-util").await;
        let exec = [("priority_level", "m-util"), ("phase", "executing")];
        let seats = format!("{NS}priority_level_seat_utilization");
        let reqs = format!("{NS}priority_level_request_utilization");
        let permit = hold(&e, "m-util", false).await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(sample_sum(&seats, &exec) > 0.0, "seat utilization held");
        assert!(sample_sum(&reqs, &exec) > 0.0, "request utilization held");
        drop(permit);
        let after = sample_sum(&seats, &exec);
        tokio::time::sleep(Duration::from_millis(20)).await;
        let later = sample_sum(&seats, &exec);
        assert!(
            (later - after).abs() < 1e-6,
            "idle adds nothing: {after} vs {later}"
        );
        assert!(has_series(
            &reqs,
            &[("priority_level", "m-util"), ("phase", "waiting")]
        ));
    }

    // noteExecutingDelta (priority-and-fairness.go:150-156): the read-vs-write
    // histograms split on the verb.
    #[tokio::test]
    async fn read_vs_write_splits_on_the_verb() {
        let (_st, e) = engine_with_level("m-rw").await;
        let name = format!("{NS}read_vs_write_current_requests");
        let mutating = [("phase", "executing"), ("request_kind", "mutating")];
        let before = sample_sum(&name, &mutating);
        let permit = hold(&e, "m-rw", true).await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(sample_sum(&name, &mutating) > before);
        drop(permit);
        assert!(has_series(
            &name,
            &[("phase", "waiting"), ("request_kind", "readOnly")]
        ));
    }
}

/// Reads samples back out of the process-global registry
/// (`legacyregistry`, which `/metrics` serves).
#[cfg(test)]
pub(crate) mod metric_test_util {
    /// The value of the counter/gauge, or the sample count of the histogram,
    /// of `name` whose labels include all of `labels` (0 when absent).
    /// Whether a series of `name` with all of `labels` exists.
    pub fn has_series(name: &str, labels: &[(&str, &str)]) -> bool {
        prometheus::default_registry()
            .gather()
            .iter()
            .filter(|mf| mf.name() == name)
            .flat_map(|mf| mf.get_metric().iter())
            .any(|m| {
                labels.iter().all(|(k, v)| {
                    m.get_label()
                        .iter()
                        .any(|l| l.name() == *k && l.value() == *v)
                })
            })
    }

    /// The sample sum of the histogram `name` whose labels include all of `labels`.
    pub fn sample_sum(name: &str, labels: &[(&str, &str)]) -> f64 {
        prometheus::default_registry()
            .gather()
            .iter()
            .filter(|mf| mf.name() == name)
            .flat_map(|mf| mf.get_metric().iter())
            .filter(|m| {
                labels.iter().all(|(k, v)| {
                    m.get_label()
                        .iter()
                        .any(|l| l.name() == *k && l.value() == *v)
                })
            })
            .map(|m| m.get_histogram().get_sample_sum())
            .sum()
    }

    pub fn sample(name: &str, labels: &[(&str, &str)]) -> f64 {
        let mut total = 0.0;
        for mf in prometheus::default_registry().gather() {
            if mf.name() != name {
                continue;
            }
            for m in mf.get_metric() {
                let ok = labels.iter().all(|(k, v)| {
                    m.get_label()
                        .iter()
                        .any(|l| l.name() == *k && l.value() == *v)
                });
                if !ok {
                    continue;
                }
                total += match mf.get_field_type() {
                    prometheus::proto::MetricType::HISTOGRAM => {
                        m.get_histogram().get_sample_count() as f64
                    }
                    prometheus::proto::MetricType::COUNTER => m.get_counter().value(),
                    _ => m.get_gauge().value(),
                };
            }
        }
        total
    }
}
