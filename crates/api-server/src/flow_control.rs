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
//! DELIBERATE DEVIATIONS (tracked as follow-up issues): no shuffle-sharded
//! fair queueing across flows (a bounded FIFO semaphore wait stands in for the
//! QueueSet), no borrowing between levels / dynamic currentCL adjustment, no
//! width (work) estimator, no metrics, no FlowSchema status updates, and the
//! engine is NOT yet installed as a request filter, so nothing is enforced.

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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::sync::Semaphore;
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
            Some(g) => g.name == "*" || d.groups.iter().any(|x| *x == g.name),
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
    exempt: bool,
    /// Concurrency limit in seats (`nominalCL`; no borrowing, see module docs).
    nominal_cl: usize,
    reject: bool,
    max_waiting: usize,
    semaphore: Arc<Semaphore>,
    waiting: AtomicUsize,
}

struct Config {
    flow_schemas: Vec<FlowSchema>,
    levels: HashMap<String, LevelState>,
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

/// Port of `digestFlowSchemasLocked` + `finishQueueSetReconfigsLocked`.
fn digest(
    mut pls: Vec<PriorityLevelConfiguration>,
    fss: Vec<FlowSchema>,
    server_cl: i64,
) -> Config {
    for name in ["exempt", "catch-all"] {
        if !pls.iter().any(|p| p.metadata.name == name) {
            if let Some(p) = mandatory_priority_level_configuration(name) {
                pls.push(p);
            }
        }
    }
    let share_sum: f64 = pls.iter().map(|p| shares_of(p) as f64).sum();
    let mut levels = HashMap::new();
    for pl in &pls {
        let exempt = matches!(pl.spec.type_, PriorityLevelType::Exempt);
        let shares = shares_of(pl) as f64;
        let cl = if share_sum > 0.0 {
            ((server_cl as f64) * shares / share_sum).ceil() as usize
        } else {
            0
        };
        let (reject, max_waiting) = match pl
            .spec
            .limited
            .as_ref()
            .and_then(|l| l.limit_response.as_ref())
        {
            Some(lr) => match (&lr.type_, &lr.queuing) {
                (LimitResponseType::Queue, Some(q)) => (
                    false,
                    (q.queues.max(0) as usize) * (q.queue_length_limit.max(0) as usize),
                ),
                (LimitResponseType::Queue, None) => (false, 0),
                _ => (true, 0),
            },
            None => (false, 0),
        };
        levels.insert(
            pl.metadata.name.clone(),
            LevelState {
                exempt,
                nominal_cl: cl,
                reject,
                max_waiting,
                semaphore: Arc::new(Semaphore::new(cl)),
                waiting: AtomicUsize::new(0),
            },
        );
    }
    // Drop FlowSchemas with a dangling priority-level ref (:762-773).
    let mut seq: Vec<FlowSchema> = fss
        .into_iter()
        .filter(|f| levels.contains_key(&f.spec.priority_level_configuration.name))
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
    Config {
        flow_schemas: seq,
        levels,
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
    _permit: Option<tokio::sync::OwnedSemaphorePermit>,
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
            config: RwLock::new(Arc::new(digest(vec![], vec![], server_cl))),
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
        *self.config.write().unwrap() = Arc::new(digest(pls, fss, self.server_cl));
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

    /// Concurrency limit (seats) of a priority level, if known.
    pub fn concurrency_limit(&self, priority_level: &str) -> Option<usize> {
        self.config
            .read()
            .unwrap()
            .levels
            .get(priority_level)
            .map(|l| l.nominal_cl)
    }

    /// Acquire `seats` for a request of the given level, waiting at most
    /// `wait_limit` for `Queue` levels.
    pub async fn execute(
        &self,
        priority_level: &str,
        seats: u32,
        wait_limit: Duration,
    ) -> Result<FlowControlPermit, FlowControlError> {
        let cfg = self.config.read().unwrap().clone();
        let Some(level) = cfg.levels.get(priority_level) else {
            return Ok(FlowControlPermit { _permit: None });
        };
        if level.exempt {
            return Ok(FlowControlPermit { _permit: None });
        }
        // Clamp a too-wide request to the level, as upstream clamps to max seats.
        let seats = seats.max(1).min(level.nominal_cl.max(1) as u32);
        if let Ok(p) = level.semaphore.clone().try_acquire_many_owned(seats) {
            return Ok(FlowControlPermit { _permit: Some(p) });
        }
        if level.reject || level.waiting.load(Ordering::SeqCst) >= level.max_waiting {
            return Err(FlowControlError::TooManyRequests);
        }
        level.waiting.fetch_add(1, Ordering::SeqCst);
        let got = tokio::time::timeout(
            wait_limit,
            level.semaphore.clone().acquire_many_owned(seats),
        )
        .await;
        level.waiting.fetch_sub(1, Ordering::SeqCst);
        match got {
            Ok(Ok(p)) => Ok(FlowControlPermit { _permit: Some(p) }),
            _ => Err(FlowControlError::TooManyRequests),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_storage::memory::MemoryStorage;

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
            .execute("exempt", 1, Duration::from_millis(1))
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn limited_level_rejects_when_seats_exhausted() {
        // catch-all is `Reject` upstream: a request beyond the limit is 429.
        let e = FlowControlEngine::with_limits(Arc::new(MemoryStorage::new()), 1, 0);
        assert_eq!(e.concurrency_limit("catch-all"), Some(1));
        let held = e
            .execute("catch-all", 1, Duration::from_millis(5))
            .await
            .unwrap();
        let second = e.execute("catch-all", 1, Duration::from_millis(5)).await;
        assert_eq!(second.err(), Some(FlowControlError::TooManyRequests));
        drop(held);
        assert!(e
            .execute("catch-all", 1, Duration::from_millis(5))
            .await
            .is_ok());
    }
}
