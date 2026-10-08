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
//! DELIBERATE DEVIATIONS (tracked as follow-up issues): no borrowing between
//! levels / dynamic currentCL adjustment (currentCL == nominalCL), no width
//! (work) estimator (callers pass seats), no metrics, no FlowSchema status
//! updates, and the engine is NOT yet installed as a request filter, so
//! nothing is enforced.
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
use std::sync::{Arc, RwLock};
use std::time::Duration;

use crate::flow_control_queueset::{
    DispatchingConfig, Execution, QueueSet, QueuingConfig, RealClock, WorkEstimate,
};
use sha2::{Digest, Sha256};
use tracing::warn;

/// `--max-requests-inflight` default (`server/config.go:443`).
pub const DEFAULT_MAX_REQUESTS_IN_FLIGHT: i64 = 400;
/// `--max-mutating-requests-inflight` default (`server/config.go:444`).
pub const DEFAULT_MAX_MUTATING_REQUESTS_IN_FLIGHT: i64 = 200;
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
    /// Concurrency limit in seats (`nominalCL`; no borrowing, see module docs).
    nominal_cl: usize,
    /// `QueuingConfig.DesiredNumQueues` (startRequest reads `Queues` for it).
    num_queues: isize,
    queues: Arc<QueueSet>,
    /// `priorityLevelState.quiescing` (apf_controller.go:215-217).
    quiescing: bool,
}

struct Config {
    flow_schemas: Vec<FlowSchema>,
    levels: HashMap<String, LevelState>,
    /// The objects this config was digested from, so a reap can re-digest
    /// without going back to storage.
    inputs: (Vec<PriorityLevelConfiguration>, Vec<FlowSchema>),
}

/// Result of classification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Classification {
    pub flow_schema: String,
    pub priority_level: String,
    pub flow_distinguisher: String,
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
) -> Config {
    /// A level of `meal.newPLStates` before `finishQueueSetReconfigsLocked`.
    struct Pending {
        pl: PriorityLevelConfiguration,
        queues: Option<Arc<QueueSet>>,
        quiescing: bool,
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
                },
            );
        }
    }

    // finishQueueSetReconfigsLocked (:846-905).
    let mut levels = HashMap::new();
    for (name, st) in new_states {
        let exempt = matches!(st.pl.spec.type_, PriorityLevelType::Exempt);
        let cl = if share_sum > 0.0 {
            ((server_cl as f64) * shares_of(&st.pl) as f64 / share_sum).ceil() as usize
        } else {
            0
        };
        let qcfg = queuing_config_for_pl(&st.pl);
        let dcfg = DispatchingConfig {
            concurrency_limit: cl,
        };
        let num_queues = qcfg.desired_num_queues;
        // validate_pl (or an earlier digest) already approved this config.
        let queues = match st.queues {
            Some(q) => {
                q.set_configuration(qcfg, dcfg)
                    .expect("queueing config was validated");
                q
            }
            None => QueueSet::new(Arc::new(RealClock::default()), qcfg, dcfg)
                .expect("queueing config was validated"),
        };
        levels.insert(
            name,
            LevelState {
                pl: st.pl,
                exempt,
                nominal_cl: cl,
                num_queues,
                queues,
                quiescing: st.quiescing,
            },
        );
    }
    Config {
        flow_schemas: seq,
        levels,
        inputs: (pls, fss),
    }
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
}

pub struct FlowControlEngine<S: Storage> {
    storage: Arc<S>,
    server_cl: i64,
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

    pub fn with_limits(storage: Arc<S>, max_inflight: i64, max_mutating_inflight: i64) -> Self {
        let server_cl = max_inflight + max_mutating_inflight;
        Self {
            storage,
            server_cl,
            config: RwLock::new(Arc::new(digest(vec![], vec![], server_cl, None))),
        }
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
        let mut cfg = self.config.write().unwrap();
        let next = digest(pls, fss, self.server_cl, Some(&cfg));
        *cfg = Arc::new(next);
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
        Classification {
            flow_schema: fs.metadata.name.clone(),
            priority_level: fs.spec.priority_level_configuration.name.clone(),
            flow_distinguisher,
        }
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
        let next = digest(pls, fss, self.server_cl, Some(&cfg));
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
        // Hold the read lock across the synchronous `start_request`
        // (upstream: startRequest runs under `cfgCtlr.lock.RLock()`), so a
        // concurrent digest cannot judge the level idle and drop it between
        // our lookup and the request registering.
        let started = {
            let guard = self.config.read().unwrap();
            let Some(level) = guard.levels.get(&c.priority_level) else {
                return Ok(FlowControlPermit { _execution: None });
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
            let we = WorkEstimate {
                initial_seats: seats.max(1) as u64,
                final_seats: 0,
                additional_latency: Duration::ZERO,
            };
            level
                .queues
                .start_request(&we, hash_value, flow_distinguisher, &c.flow_schema)
        };
        let handle = match started {
            Ok(h) => h,
            Err(rejected) => {
                // `if idle { maybeReapReadLocked }` (apf_controller.go:1075-1077)
                if rejected.idle {
                    self.maybe_reap(&c.priority_level);
                }
                return Err(FlowControlError::TooManyRequests);
            }
        };
        // Dropping the wait future on timeout cancels the queued request.
        match tokio::time::timeout(wait_limit, handle.wait()).await {
            Ok(exec) => {
                exec.note_dispatched();
                Ok(FlowControlPermit {
                    _execution: Some(exec),
                })
            }
            Err(_) => Err(FlowControlError::TooManyRequests),
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
}
