//! `kubectl cordon`, `kubectl uncordon` and `kubectl drain`.
//!
//! Faithful port of upstream (release-1.35):
//!
//! - `staging/src/k8s.io/kubectl/pkg/cmd/drain/drain.go` -- the commands
//!   (`DrainCmdOptions`, `Complete`, `RunDrain`, `deleteOrEvictPodsSimple`,
//!   `RunCordonOrUncordon`, the `onPodDeletionOrEvictionFinished` printer).
//! - `staging/src/k8s.io/kubectl/pkg/drain/drain.go` -- `Helper`
//!   (`CheckEvictionSupport`, `GetPodsForDeletion`, `DeleteOrEvictPods`,
//!   `evictPods`, `deletePods`, `waitForDelete`, `makeDeleteOptions`).
//! - `staging/src/k8s.io/kubectl/pkg/drain/filters.go` -- the pod filters
//!   (`skipDeletedFilter`, `daemonSetFilter`, `mirrorPodFilter`,
//!   `localStorageFilter`, `unreplicatedFilter`), `PodDeleteList`.
//! - `staging/src/k8s.io/kubectl/pkg/drain/cordon.go` -- `CordonHelper`.
//! - `staging/src/k8s.io/cli-runtime/pkg/printers/name.go` and
//!   `staging/src/k8s.io/kubectl/pkg/cmd/util/helpers.go`
//!   (`GetDryRunStrategy`, `PrintFlagsWithDryRunStrategy`) -- output lines.
//!
//! Intentional, documented differences (no Rust/Go-equivalent surface):
//!
//! - `Helper.AdditionalFilters` is kept (library hook) but unused by the CLI,
//!   exactly as upstream's `kubectl`.
//! - `Helper.OnPodDeletedOrEvicted` (deprecated; only consulted when
//!   `OnPodDeletionOrEvictionFinished` is nil, which the command never leaves
//!   nil) is not ported.
//! - `OnPodDeletionOrEvictionStarted` is wired but the command only prints at
//!   klog verbosity >= 2 (`klog.V(2).Enabled()`); this CLI has no `-v` flag, so
//!   the hook is never installed.
//! - `CordonHelper.PatchOrReplace`'s `Update` fallback is taken only when
//!   `strategicpatch.CreateTwoWayMergePatch` fails; the patch for the single
//!   `spec.unschedulable` field is built directly and cannot fail, so the
//!   fallback is unreachable and not ported.
//! - The resource builder (`f.NewBuilder()...ResourceNames("nodes", ...)`)
//!   is reduced to its observable behaviour for nodes: `NAME`/`node/NAME`
//!   lookups, `--selector` lists with `--chunk-size` continuation.

use anyhow::{anyhow, Result};
use k3rs_client::api_status::{
    has_status_cause, is_bad_request, is_forbidden, is_not_found, is_resource_expired,
    is_too_many_requests, ApiStatusError,
};
use k3rs_client::http::{ApiClient, GetError};
use k3rs_common::label_selector::LabelSelector;
use serde_json::{json, Value};
use std::io::IsTerminal;
use std::sync::Arc;
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Output sinks (`Helper.Out` / `Helper.ErrOut`)
// ---------------------------------------------------------------------------

/// A writer for command output. Each call receives text that already carries
/// its trailing newline (the Go code `Fprintf`s with explicit `\n`).
pub type Sink = Arc<dyn Fn(&str) + Send + Sync>;

/// `genericiooptions.IOStreams`' `Out`/`ErrOut`.
#[derive(Clone)]
pub struct IoStreams {
    pub out: Sink,
    pub err_out: Sink,
}

impl IoStreams {
    /// `genericiooptions.IOStreams{Out: os.Stdout, ErrOut: os.Stderr}`.
    pub fn std() -> Self {
        Self {
            out: Arc::new(|s| print!("{s}")),
            err_out: Arc::new(|s| eprint!("{s}")),
        }
    }
}

// ---------------------------------------------------------------------------
// cmdutil.DryRunStrategy
// ---------------------------------------------------------------------------

/// `cmdutil.DryRunStrategy` (kubectl/pkg/cmd/util/helpers.go).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DryRunStrategy {
    None,
    Client,
    Server,
}

impl DryRunStrategy {
    /// `cmdutil.GetDryRunStrategy` (helpers.go). `unchanged` is the
    /// flag's `NoOptDefVal` (bare `--dry-run`).
    pub fn parse(flag: &str) -> Result<Self> {
        match flag.parse::<bool>() {
            Err(_) => match flag {
                "unchanged" | "client" => Ok(Self::Client),
                "server" => Ok(Self::Server),
                "none" => Ok(Self::None),
                _ => Err(anyhow!(
                    "Invalid dry-run value ({flag}). Must be \"none\", \"server\", or \"client\"."
                )),
            },
            Ok(true) => Ok(Self::Client),
            Ok(false) => Ok(Self::None),
        }
    }

    /// `PrintFlagsWithDryRunStrategy` (helpers.go): the operation
    /// template applied to the name printer.
    fn decorate(self, operation: &str) -> String {
        match self {
            Self::Client => format!("{operation} (dry run)"),
            Self::Server => format!("{operation} (server dry run)"),
            Self::None => operation.to_string(),
        }
    }
}

// ---------------------------------------------------------------------------
// Small Go-parity helpers
// ---------------------------------------------------------------------------

/// Go's `%q` for the DNS-safe names that reach it.
fn q(s: &str) -> String {
    format!("{s:?}")
}

/// `err.Error()` of a client-go `StatusError` is the bare Status message; our
/// client renders it as `Error from server (Reason): message`.
fn err_message(err: &anyhow::Error) -> String {
    let full = err.to_string();
    if err
        .chain()
        .any(|c| c.downcast_ref::<ApiStatusError>().is_some())
    {
        if let Some(rest) = full.strip_prefix("Error from server") {
            if let Some(idx) = rest.find("): ") {
                if rest.starts_with(" (") {
                    return rest[idx + 3..].to_string();
                }
            } else if let Some(m) = rest.strip_prefix(": ") {
                return m.to_string();
            }
        }
    }
    full
}

/// `utilerrors.NewAggregate(errs).Error()` (apimachinery
/// pkg/util/errors/errors.go): one error renders as itself, several as
/// `[a, b]` with duplicates dropped.
fn aggregate_message(errs: &[String]) -> String {
    if errs.len() == 1 {
        return errs[0].clone();
    }
    let mut seen: Vec<&str> = Vec::new();
    for e in errs {
        if !seen.contains(&e.as_str()) {
            seen.push(e);
        }
    }
    if seen.len() == 1 {
        return seen[0].to_string();
    }
    format!("[{}]", seen.join(", "))
}

fn aggregate(errs: &[String]) -> Option<anyhow::Error> {
    if errs.is_empty() {
        None
    } else {
        Some(anyhow!(aggregate_message(errs)))
    }
}

/// Go `time.Duration.String()`.
pub fn format_go_duration(d: Duration) -> String {
    let nanos = d.as_nanos();
    if nanos == 0 {
        return "0s".to_string();
    }
    if nanos < 1_000 {
        return format!("{nanos}ns");
    }
    if nanos < 1_000_000 {
        return format!("{}µs", trim_frac(nanos as f64 / 1e3));
    }
    if nanos < 1_000_000_000 {
        return format!("{}ms", trim_frac(nanos as f64 / 1e6));
    }
    let total_secs = d.as_secs();
    let frac = d.subsec_nanos();
    let h = total_secs / 3600;
    let m = (total_secs % 3600) / 60;
    let s = total_secs % 60;
    let secs = if frac == 0 {
        format!("{s}s")
    } else {
        let f = format!("{frac:09}");
        format!("{s}.{}s", f.trim_end_matches('0'))
    };
    if h > 0 {
        format!("{h}h{m}m{secs}")
    } else if m > 0 {
        format!("{m}m{secs}")
    } else {
        secs
    }
}

fn trim_frac(v: f64) -> String {
    let s = format!("{v:.6}");
    s.trim_end_matches('0').trim_end_matches('.').to_string()
}

/// Go `time.ParseDuration` (used by the `--timeout` `DurationVar` flag).
pub fn parse_go_duration(s: &str) -> std::result::Result<Duration, String> {
    let orig = s;
    let bad = || format!("time: invalid duration {}", q(orig));
    let mut rest = s;
    let mut neg = false;
    if let Some(r) = rest.strip_prefix('-') {
        neg = true;
        rest = r;
    } else if let Some(r) = rest.strip_prefix('+') {
        rest = r;
    }
    if rest == "0" {
        return Ok(Duration::ZERO);
    }
    if rest.is_empty() {
        return Err(bad());
    }
    let mut total = 0f64;
    while !rest.is_empty() {
        let num_end = rest
            .find(|c: char| !(c.is_ascii_digit() || c == '.'))
            .unwrap_or(rest.len());
        if num_end == 0 {
            return Err(bad());
        }
        let v: f64 = rest[..num_end].parse().map_err(|_| bad())?;
        rest = &rest[num_end..];
        let unit_end = rest
            .find(|c: char| c.is_ascii_digit() || c == '.')
            .unwrap_or(rest.len());
        let unit = &rest[..unit_end];
        rest = &rest[unit_end..];
        let mult = match unit {
            "ns" => 1.0,
            "us" | "µs" | "μs" => 1e3,
            "ms" => 1e6,
            "s" => 1e9,
            "m" => 60e9,
            "h" => 3600e9,
            "" => {
                return Err(format!("time: missing unit in duration {}", q(orig)));
            }
            _ => {
                return Err(format!(
                    "time: unknown unit {} in duration {}",
                    q(unit),
                    q(orig)
                ));
            }
        };
        total += v * mult;
    }
    if neg {
        // Durations are non-negative here; a negative timeout never expires
        // before it starts in Go either, but cannot be represented.
        return Err(bad());
    }
    Ok(Duration::from_nanos(total as u64))
}

// ---------------------------------------------------------------------------
// Pod accessors (the Go code reads typed `corev1.Pod` fields)
// ---------------------------------------------------------------------------

fn pod_namespace(pod: &Value) -> &str {
    pod.pointer("/metadata/namespace")
        .and_then(|v| v.as_str())
        .unwrap_or("")
}

fn pod_name(pod: &Value) -> &str {
    pod.pointer("/metadata/name")
        .and_then(|v| v.as_str())
        .unwrap_or("")
}

fn pod_uid(pod: &Value) -> &str {
    pod.pointer("/metadata/uid")
        .and_then(|v| v.as_str())
        .unwrap_or("")
}

fn pod_phase(pod: &Value) -> &str {
    pod.pointer("/status/phase")
        .and_then(|v| v.as_str())
        .unwrap_or("")
}

/// `pod.ObjectMeta.DeletionTimestamp` (None when unset, i.e. `IsZero()`).
fn deletion_timestamp(pod: &Value) -> Option<chrono::DateTime<chrono::Utc>> {
    let s = pod.pointer("/metadata/deletionTimestamp")?.as_str()?;
    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|t| t.with_timezone(&chrono::Utc))
}

/// `metav1.GetControllerOf` (apimachinery pkg/apis/meta/v1/controller_ref.go):
/// the owner reference with `controller: true`.
fn controller_of(pod: &Value) -> Option<&Value> {
    pod.pointer("/metadata/ownerReferences")?
        .as_array()?
        .iter()
        .find(|r| r.get("controller").and_then(|c| c.as_bool()) == Some(true))
}

fn is_finished(pod: &Value) -> bool {
    matches!(pod_phase(pod), "Succeeded" | "Failed")
}

// ---------------------------------------------------------------------------
// filters.go
// ---------------------------------------------------------------------------

const DAEMON_SET_FATAL: &str = "DaemonSet-managed Pods (use --ignore-daemonsets to ignore)";
const DAEMON_SET_WARNING: &str = "ignoring DaemonSet-managed Pods";
const LOCAL_STORAGE_FATAL: &str =
    "Pods with local storage (use --delete-emptydir-data to override)";
const LOCAL_STORAGE_WARNING: &str = "deleting Pods with local storage";
const UNMANAGED_FATAL: &str =
    "cannot delete Pods that declare no controller (use --force to override)";
const UNMANAGED_WARNING: &str = "deleting Pods that declare no controller";

pub const POD_DELETE_STATUS_TYPE_OKAY: &str = "Okay";
pub const POD_DELETE_STATUS_TYPE_SKIP: &str = "Skip";
pub const POD_DELETE_STATUS_TYPE_WARNING: &str = "Warning";
pub const POD_DELETE_STATUS_TYPE_ERROR: &str = "Error";

/// `PodDeleteStatus` (filters.go).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PodDeleteStatus {
    pub delete: bool,
    pub reason: &'static str,
    pub message: String,
}

impl PodDeleteStatus {
    pub fn okay() -> Self {
        Self {
            delete: true,
            reason: POD_DELETE_STATUS_TYPE_OKAY,
            message: String::new(),
        }
    }
    pub fn skip() -> Self {
        Self {
            delete: false,
            reason: POD_DELETE_STATUS_TYPE_SKIP,
            message: String::new(),
        }
    }
    pub fn with_warning(delete: bool, message: impl Into<String>) -> Self {
        Self {
            delete,
            reason: POD_DELETE_STATUS_TYPE_WARNING,
            message: message.into(),
        }
    }
    pub fn with_error(message: impl Into<String>) -> Self {
        Self {
            delete: false,
            reason: POD_DELETE_STATUS_TYPE_ERROR,
            message: message.into(),
        }
    }
}

/// `PodFilter` (filters.go): `func(corev1.Pod) PodDeleteStatus`.
pub type PodFilter = Arc<dyn Fn(&Value) -> PodDeleteStatus + Send + Sync>;

/// `PodDelete` (filters.go).
#[derive(Clone, Debug)]
pub struct PodDelete {
    pub pod: Value,
    pub status: PodDeleteStatus,
}

/// `PodDeleteList` (filters.go).
#[derive(Clone, Debug, Default)]
pub struct PodDeleteList {
    items: Vec<PodDelete>,
}

impl PodDeleteList {
    /// `Pods()`: every pod marked for deletion after filtering.
    pub fn pods(&self) -> Vec<Value> {
        self.items
            .iter()
            .filter(|i| i.status.delete)
            .map(|i| i.pod.clone())
            .collect()
    }

    /// `Warnings()`: all warning messages concatenated into a string.
    pub fn warnings(&self) -> String {
        // Go iterates a map (random order); insertion order is deterministic.
        let mut ps: Vec<(String, Vec<String>)> = Vec::new();
        for i in &self.items {
            if i.status.reason == POD_DELETE_STATUS_TYPE_WARNING {
                let name = format!("{}/{}", pod_namespace(&i.pod), pod_name(&i.pod));
                match ps.iter_mut().find(|(m, _)| *m == i.status.message) {
                    Some((_, v)) => v.push(name),
                    None => ps.push((i.status.message.clone(), vec![name])),
                }
            }
        }
        ps.iter()
            .map(|(k, v)| format!("{k}: {}", v.join(", ")))
            .collect::<Vec<_>>()
            .join("; ")
    }

    /// `errors()`.
    pub fn errors(&self) -> Vec<String> {
        let mut failed: Vec<(String, Vec<String>)> = Vec::new();
        for i in &self.items {
            if i.status.reason == POD_DELETE_STATUS_TYPE_ERROR {
                let msg = if i.status.message.is_empty() {
                    "unexpected error".to_string()
                } else {
                    i.status.message.clone()
                };
                let name = format!("{}/{}", pod_namespace(&i.pod), pod_name(&i.pod));
                match failed.iter_mut().find(|(m, _)| *m == msg) {
                    Some((_, v)) => v.push(name),
                    None => failed.push((msg, vec![name])),
                }
            }
        }
        failed
            .into_iter()
            .map(|(msg, pods)| format!("cannot delete {msg}: {}", pods.join(", ")))
            .collect()
    }
}

fn has_local_storage(pod: &Value) -> bool {
    pod.pointer("/spec/volumes")
        .and_then(|v| v.as_array())
        .is_some_and(|vols| {
            vols.iter()
                .any(|v| v.get("emptyDir").is_some_and(|e| !e.is_null()))
        })
}

/// `isControllerRefDaemonSet` (filters.go).
fn is_controller_ref_daemon_set(r: &Value) -> bool {
    r.get("kind").and_then(|k| k.as_str()) == Some("DaemonSet")
        && r.get("apiVersion").and_then(|k| k.as_str()) == Some("apps/v1")
}

/// `shouldSkipPod` (filters.go).
fn should_skip_pod(pod: &Value, skip_deleted_timeout_seconds: i64) -> bool {
    if skip_deleted_timeout_seconds <= 0 {
        return false;
    }
    match deletion_timestamp(pod) {
        Some(ts) => {
            let secs = chrono::Utc::now().signed_duration_since(ts).num_seconds();
            secs > skip_deleted_timeout_seconds
        }
        None => false,
    }
}

// ---------------------------------------------------------------------------
// drain.go -- Helper
// ---------------------------------------------------------------------------

/// `EvictionKind` / `EvictionSubresource` (drain.go).
const EVICTION_KIND: &str = "Eviction";
const EVICTION_SUBRESOURCE: &str = "pods/eviction";

/// `corev1.NamespaceTerminatingCause`.
const NAMESPACE_TERMINATING_CAUSE: &str = "NamespaceTerminating";

/// The group/version `CheckEvictionSupport` returns (`schema.GroupVersion`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GroupVersion {
    pub group: String,
    pub version: String,
}

impl GroupVersion {
    fn is_empty(&self) -> bool {
        self.group.is_empty() && self.version.is_empty()
    }
}

type FinishedFn = Arc<dyn Fn(&Value, bool, Option<&anyhow::Error>) + Send + Sync>;
type StartedFn = Arc<dyn Fn(&Value, bool) + Send + Sync>;

/// `drain.Helper` (kubectl/pkg/drain/drain.go).
pub struct Helper<'a> {
    pub client: &'a ApiClient,
    pub force: bool,
    /// IMPORTANT (upstream): 0 means "delete immediately"; negative uses the
    /// pod's `terminationGracePeriodSeconds`.
    pub grace_period_seconds: i64,
    pub ignore_all_daemon_sets: bool,
    /// Zero means infinite.
    pub timeout: Duration,
    pub delete_empty_dir_data: bool,
    pub selector: String,
    pub pod_selector: String,
    pub chunk_size: i64,
    pub disable_eviction: bool,
    pub skip_wait_for_delete_timeout_seconds: i64,
    pub evict_error_retry_delay: Duration,
    pub additional_filters: Vec<PodFilter>,
    pub out: Sink,
    pub err_out: Sink,
    pub dry_run_strategy: DryRunStrategy,
    pub on_pod_deletion_or_eviction_finished: Option<FinishedFn>,
    pub on_pod_deletion_or_eviction_started: Option<StartedFn>,
}

/// The context deadline `context.WithTimeout` gives `evictPods`/`waitForDelete`.
#[derive(Clone, Copy)]
struct Deadline(Option<Instant>);

impl Deadline {
    fn expired(&self) -> bool {
        self.0.is_some_and(|d| Instant::now() >= d)
    }
}

/// `wait.PollUntilContextTimeout(ctx, interval, timeout, immediate=true, ...)`
/// error text on expiry.
const CONTEXT_DEADLINE_EXCEEDED: &str = "context deadline exceeded";

impl<'a> Helper<'a> {
    /// The defaults `NewDrainCmdOptions` (cmd/drain/drain.go) gives
    /// the helper.
    pub fn new(client: &'a ApiClient, streams: &IoStreams) -> Self {
        Self {
            client,
            force: false,
            grace_period_seconds: -1,
            ignore_all_daemon_sets: false,
            timeout: Duration::ZERO,
            delete_empty_dir_data: false,
            selector: String::new(),
            pod_selector: String::new(),
            // cmdutil.DefaultChunkSize
            chunk_size: 500,
            disable_eviction: false,
            skip_wait_for_delete_timeout_seconds: 0,
            evict_error_retry_delay: Duration::from_secs(5),
            additional_filters: Vec::new(),
            out: streams.out.clone(),
            err_out: streams.err_out.clone(),
            dry_run_strategy: DryRunStrategy::None,
            on_pod_deletion_or_eviction_finished: None,
            on_pod_deletion_or_eviction_started: None,
        }
    }

    /// `CheckEvictionSupport` (drain.go): discovery of `v1` for the
    /// `pods/eviction` subresource and its group/version.
    pub async fn check_eviction_support(&self) -> Result<GroupVersion> {
        let list: Value = self
            .client
            .get("/api/v1")
            .await
            .map_err(|e| anyhow!("{e}"))?;
        if let Some(resources) = list.get("resources").and_then(|r| r.as_array()) {
            for r in resources {
                let s = |k: &str| r.get(k).and_then(|v| v.as_str()).unwrap_or("");
                if s("name") == EVICTION_SUBRESOURCE
                    && s("kind") == EVICTION_KIND
                    && !s("group").is_empty()
                    && !s("version").is_empty()
                {
                    return Ok(GroupVersion {
                        group: s("group").to_string(),
                        version: s("version").to_string(),
                    });
                }
            }
        }
        Ok(GroupVersion {
            group: String::new(),
            version: String::new(),
        })
    }

    /// `makeDeleteOptions` (drain.go).
    fn make_delete_options(&self) -> Value {
        let mut opts = serde_json::Map::new();
        if self.grace_period_seconds >= 0 {
            opts.insert(
                "gracePeriodSeconds".into(),
                json!(self.grace_period_seconds),
            );
        }
        if self.dry_run_strategy == DryRunStrategy::Server {
            opts.insert("dryRun".into(), json!(["All"]));
        }
        Value::Object(opts)
    }

    /// `DeletePod` (drain.go). A 404 comes back from the client as a
    /// status, not an error; the caller treats it as `IsNotFound`.
    pub async fn delete_pod(&self, pod: &Value) -> Result<reqwest::StatusCode> {
        let path = format!(
            "/api/v1/namespaces/{}/pods/{}",
            pod_namespace(pod),
            pod_name(pod)
        );
        self.client
            .delete_with_options(&path, &[], Some(&self.make_delete_options()))
            .await
    }

    /// `EvictPod` (drain.go): `policy/v1` when the server supports it,
    /// else `policy/v1beta1`.
    pub async fn evict_pod(
        &self,
        pod: &Value,
        eviction_group_version: &GroupVersion,
    ) -> Result<()> {
        let api_version =
            if eviction_group_version.group == "policy" && eviction_group_version.version == "v1" {
                "policy/v1"
            } else {
                "policy/v1beta1"
            };
        let eviction = json!({
            "apiVersion": api_version,
            "kind": EVICTION_KIND,
            "metadata": {
                "name": pod_name(pod),
                "namespace": pod_namespace(pod),
            },
            "deleteOptions": self.make_delete_options(),
        });
        let path = format!(
            "/api/v1/namespaces/{}/pods/{}/eviction",
            pod_namespace(pod),
            pod_name(pod)
        );
        let _: Value = self.client.post(&path, &eviction).await?;
        Ok(())
    }

    /// `makeFilters` (filters.go): the order matters, only the last
    /// filter's message is retained when there are warnings.
    async fn run_filters(&self, pod: &Value) -> PodDeleteStatus {
        let mut status = self.skip_deleted_filter(pod);
        if status.delete {
            status = self.daemon_set_filter(pod).await;
        }
        if status.delete {
            status = Self::mirror_pod_filter(pod);
        }
        if status.delete {
            status = self.local_storage_filter(pod);
        }
        if status.delete {
            status = self.unreplicated_filter(pod);
        }
        for f in &self.additional_filters {
            if !status.delete {
                break;
            }
            status = f(pod);
        }
        status
    }

    /// `skipDeletedFilter` (filters.go).
    fn skip_deleted_filter(&self, pod: &Value) -> PodDeleteStatus {
        if should_skip_pod(pod, self.skip_wait_for_delete_timeout_seconds) {
            PodDeleteStatus::skip()
        } else {
            PodDeleteStatus::okay()
        }
    }

    /// `daemonSetFilter` (filters.go).
    async fn daemon_set_filter(&self, pod: &Value) -> PodDeleteStatus {
        // Note that we return false in cases where the pod is DaemonSet
        // managed, regardless of flags. The exception is orphaned pods (the
        // DaemonSet is not found): those are deleted if --force is used.
        let Some(controller_ref) = controller_of(pod) else {
            return PodDeleteStatus::okay();
        };
        if !is_controller_ref_daemon_set(controller_ref) {
            return PodDeleteStatus::okay();
        }
        // Any finished pod can be removed.
        if is_finished(pod) {
            return PodDeleteStatus::okay();
        }
        let ds_name = controller_ref
            .get("name")
            .and_then(|n| n.as_str())
            .unwrap_or("");
        let path = format!(
            "/apis/apps/v1/namespaces/{}/daemonsets/{}",
            pod_namespace(pod),
            ds_name
        );
        match self.client.get::<Value>(&path).await {
            Ok(_) => {}
            Err(GetError::NotFound(status)) => {
                // filters.go daemonSetFilter: `apierrors.IsNotFound(err)`;
                // the message is the server's `err.Error()`, not synthesized.
                let msg = err_message(&anyhow::Error::new(status));
                // remove orphaned pods with a warning if --force is used
                if self.force {
                    return PodDeleteStatus::with_warning(true, msg);
                }
                return PodDeleteStatus::with_error(msg);
            }
            Err(GetError::Other(e)) => return PodDeleteStatus::with_error(err_message(&e)),
        }
        if !self.ignore_all_daemon_sets {
            return PodDeleteStatus::with_error(DAEMON_SET_FATAL);
        }
        PodDeleteStatus::with_warning(false, DAEMON_SET_WARNING)
    }

    /// `mirrorPodFilter` (filters.go).
    fn mirror_pod_filter(pod: &Value) -> PodDeleteStatus {
        let found = pod
            .pointer("/metadata/annotations")
            .and_then(|a| a.as_object())
            // corev1.MirrorPodAnnotationKey
            .is_some_and(|a| a.contains_key("kubernetes.io/config.mirror"));
        if found {
            PodDeleteStatus::skip()
        } else {
            PodDeleteStatus::okay()
        }
    }

    /// `localStorageFilter` (filters.go).
    fn local_storage_filter(&self, pod: &Value) -> PodDeleteStatus {
        if !has_local_storage(pod) {
            return PodDeleteStatus::okay();
        }
        // Any finished pod can be removed.
        if is_finished(pod) {
            return PodDeleteStatus::okay();
        }
        if !self.delete_empty_dir_data {
            return PodDeleteStatus::with_error(LOCAL_STORAGE_FATAL);
        }
        // TODO (upstream): this warning gets dropped by subsequent filters.
        PodDeleteStatus::with_warning(true, LOCAL_STORAGE_WARNING)
    }

    /// `unreplicatedFilter` (filters.go).
    fn unreplicated_filter(&self, pod: &Value) -> PodDeleteStatus {
        // any finished pod can be removed
        if is_finished(pod) {
            return PodDeleteStatus::okay();
        }
        if controller_of(pod).is_some() {
            return PodDeleteStatus::okay();
        }
        if self.force {
            return PodDeleteStatus::with_warning(true, UNMANAGED_WARNING);
        }
        PodDeleteStatus::with_error(UNMANAGED_FATAL)
    }

    /// `GetPodsForDeletion` (drain.go): the pods bound to `node_name`
    /// (chunked LIST with `FollowContinue`), run through the filters. The
    /// list is returned alongside any filter errors, as upstream does.
    pub async fn get_pods_for_deletion(
        &self,
        node_name: &str,
    ) -> (Option<PodDeleteList>, Vec<String>) {
        let label_selector = match LabelSelector::parse(&self.pod_selector) {
            Ok(s) => s.to_string(),
            Err(e) => return (None, vec![e.to_string()]),
        };
        let field_selector = format!("spec.nodeName={node_name}");

        let mut items: Vec<Value> = Vec::new();
        let mut continue_token = String::new();
        loop {
            let mut query = url::form_urlencoded::Serializer::new(String::new());
            if !label_selector.is_empty() {
                query.append_pair("labelSelector", &label_selector);
            }
            query.append_pair("fieldSelector", &field_selector);
            if self.chunk_size > 0 {
                query.append_pair("limit", &self.chunk_size.to_string());
            }
            if !continue_token.is_empty() {
                query.append_pair("continue", &continue_token);
            }
            let path = format!("/api/v1/pods?{}", query.finish());
            let page: Value = match self.client.get(&path).await {
                Ok(v) => v,
                Err(e) => {
                    let e = match e {
                        GetError::NotFound(_) => anyhow!("Resource not found"),
                        GetError::Other(e) => e,
                    };
                    // resource.EnhanceListError(err, options, "/v1, Resource=pods")
                    let e = enhance_list_error(
                        e,
                        &label_selector,
                        &field_selector,
                        "/v1, Resource=pods",
                    );
                    return (None, vec![err_message(&e)]);
                }
            };
            if let Some(arr) = page.get("items").and_then(|i| i.as_array()) {
                items.extend(arr.iter().cloned());
            }
            continue_token = page
                .pointer("/metadata/continue")
                .and_then(|c| c.as_str())
                .unwrap_or("")
                .to_string();
            if continue_token.is_empty() {
                break;
            }
        }

        let list = self.filter_pods(items).await;
        let errs = list.errors();
        if !errs.is_empty() {
            return (Some(list), errs);
        }
        (Some(list), Vec::new())
    }

    /// `filterPods` (drain.go).
    async fn filter_pods(&self, pods: Vec<Value>) -> PodDeleteList {
        let mut out = Vec::new();
        for mut pod in pods {
            let status = self.run_filters(&pod).await;
            // Add the pod no matter what PodDeleteStatus is: pods whose
            // status is not Delete (like DaemonSet) are caught by errors().
            if let Some(o) = pod.as_object_mut() {
                o.insert("kind".into(), json!("Pod"));
                o.insert("apiVersion".into(), json!("v1"));
            }
            out.push(PodDelete { pod, status });
        }
        PodDeleteList { items: out }
    }

    async fn get_pod(&self, namespace: &str, name: &str) -> std::result::Result<Value, GetError> {
        self.client
            .get(&format!("/api/v1/namespaces/{namespace}/pods/{name}"))
            .await
    }

    /// `DeleteOrEvictPods` (drain.go): evict when the server supports
    /// it (and `--disable-eviction` is unset), else delete.
    pub async fn delete_or_evict_pods(&self, pods: &[Value]) -> Result<()> {
        if pods.is_empty() {
            return Ok(());
        }
        if !self.disable_eviction {
            let gv = self.check_eviction_support().await?;
            if !gv.is_empty() {
                return self.evict_pods(pods, &gv).await;
            }
        }
        self.delete_pods(pods).await
    }

    /// 0 timeout means infinite.
    fn global_timeout(&self) -> (Duration, Deadline) {
        if self.timeout.is_zero() {
            (self.timeout, Deadline(None))
        } else {
            (self.timeout, Deadline(Some(Instant::now() + self.timeout)))
        }
    }

    /// `evictPods` (drain.go): one concurrent worker per pod, errors
    /// aggregated.
    async fn evict_pods(&self, pods: &[Value], gv: &GroupVersion) -> Result<()> {
        let (global_timeout, deadline) = self.global_timeout();
        let workers = pods
            .iter()
            .map(|pod| self.evict_one(pod, gv, global_timeout, deadline));
        let results = futures::future::join_all(workers).await;
        let errors: Vec<String> = results
            .into_iter()
            .filter_map(|r| r.err().map(|e| e.to_string()))
            .collect();
        match aggregate(&errors) {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// The per-pod goroutine of `evictPods` (drain.go).
    async fn evict_one(
        &self,
        pod: &Value,
        gv: &GroupVersion,
        global_timeout: Duration,
        deadline: Deadline,
    ) -> Result<()> {
        let mut active_pod = pod.clone();
        loop {
            let ns = pod_namespace(&active_pod).to_string();
            let name = pod_name(&active_pod).to_string();
            if self.dry_run_strategy == DryRunStrategy::Server {
                (self.out)(&format!("evicting pod {ns}/{name} (server dry run)\n"));
            } else {
                if let Some(f) = &self.on_pod_deletion_or_eviction_started {
                    f(&active_pod, true);
                }
                (self.out)(&format!("evicting pod {ns}/{name}\n"));
            }
            if deadline.expired() {
                return Err(anyhow!(
                    "error when evicting pods/{} -n {}: global timeout reached: {}",
                    q(&name),
                    q(&ns),
                    format_go_duration(global_timeout)
                ));
            }

            match self.evict_pod(&active_pod, gv).await {
                Ok(()) => break,
                Err(err) => {
                    if is_not_found(&err) {
                        return Ok(());
                    } else if is_too_many_requests(&err) {
                        (self.err_out)(&format!(
                            "error when evicting pods/{} -n {} (will retry after {}): {}\n",
                            q(&name),
                            q(&ns),
                            format_go_duration(self.evict_error_retry_delay),
                            err_message(&err)
                        ));
                        tokio::time::sleep(self.evict_error_retry_delay).await;
                    } else if deletion_timestamp(&active_pod).is_some()
                        && is_forbidden(&err)
                        && has_status_cause(&err, NAMESPACE_TERMINATING_CAUSE)
                    {
                        // an eviction request in a deleting namespace will
                        // throw a forbidden error, if the pod is already marked
                        // deleted, we can ignore this error, an eviction
                        // request will never succeed, but we will
                        // waitForDelete for this pod.
                        break;
                    } else if is_forbidden(&err)
                        && has_status_cause(&err, NAMESPACE_TERMINATING_CAUSE)
                    {
                        // an eviction request in a deleting namespace will
                        // throw a forbidden error, if the pod is not marked
                        // deleted, we retry until it is.
                        (self.err_out)(&format!(
                            "error when evicting pod {} from terminating namespace {} (will retry after {}): {}\n",
                            q(&name),
                            q(&ns),
                            format_go_duration(self.evict_error_retry_delay),
                            err_message(&err)
                        ));
                        tokio::time::sleep(self.evict_error_retry_delay).await;
                    } else {
                        return Err(anyhow!(
                            "error when evicting pods/{} -n {}: {}",
                            q(&name),
                            q(&ns),
                            err_message(&err)
                        ));
                    }
                }
            }

            // we ignore errors and let eviction sort it out with the original pod.
            if let Ok(fresh) = self.get_pod(&ns, &name).await {
                active_pod = fresh;
            }
        }

        if self.dry_run_strategy == DryRunStrategy::Server {
            return Ok(());
        }
        match self
            .wait_for_delete(
                std::slice::from_ref(&active_pod),
                Duration::from_secs(1),
                None,
                true,
                deadline,
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => Err(anyhow!(
                "error when waiting for pod {} in namespace {} to terminate: {}",
                q(pod_name(pod)),
                q(pod_namespace(pod)),
                e
            )),
        }
    }

    /// `deletePods` (drain.go).
    async fn delete_pods(&self, pods: &[Value]) -> Result<()> {
        // 0 timeout means infinite.
        let timeout = if self.timeout.is_zero() {
            None
        } else {
            Some(self.timeout)
        };
        for pod in pods {
            match self.delete_pod(pod).await {
                Ok(_) => {}
                Err(e) if is_not_found(&e) => {}
                Err(e) => return Err(e),
            }
            if let Some(f) = &self.on_pod_deletion_or_eviction_started {
                f(pod, false);
            }
        }
        self.wait_for_delete(pods, Duration::from_secs(1), timeout, false, Deadline(None))
            .await
            .map(|_| ())
            .map_err(|e| anyhow!("{e}"))
    }

    /// `waitForDelete` (drain.go): poll (immediately, then every
    /// `interval`) until every pod is gone, replaced (UID changed) or skipped.
    /// `timeout` is the poll timeout; `ctx` the enclosing context deadline.
    async fn wait_for_delete(
        &self,
        pods: &[Value],
        interval: Duration,
        timeout: Option<Duration>,
        using_eviction: bool,
        ctx: Deadline,
    ) -> std::result::Result<Vec<Value>, String> {
        let poll_deadline = timeout.map(|t| Instant::now() + t);
        let effective = match (ctx.0, poll_deadline) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        let mut pods: Vec<Value> = pods.to_vec();
        loop {
            let mut pending: Vec<Value> = Vec::new();
            for pod in &pods {
                match self.get_pod(pod_namespace(pod), pod_name(pod)).await {
                    Err(GetError::NotFound(_)) => {
                        self.finished(pod, using_eviction, None);
                    }
                    Ok(p) if pod_uid(&p) != pod_uid(pod) => {
                        self.finished(pod, using_eviction, None);
                    }
                    Err(GetError::Other(e)) => {
                        self.finished(pod, using_eviction, Some(&e));
                        return Err(e.to_string());
                    }
                    Ok(p) => {
                        if should_skip_pod(&p, self.skip_wait_for_delete_timeout_seconds) {
                            (self.out)(&format!(
                                "pod {} has DeletionTimestamp older than {} seconds, skipping\n",
                                q(pod_name(pod)),
                                self.skip_wait_for_delete_timeout_seconds
                            ));
                            continue;
                        }
                        pending.push(pod.clone());
                    }
                }
            }
            pods = pending;
            if pods.is_empty() {
                return Ok(pods);
            }
            match effective {
                Some(d) => {
                    let wake = Instant::now() + interval;
                    if wake >= d {
                        tokio::time::sleep_until(d.into()).await;
                        return Err(CONTEXT_DEADLINE_EXCEEDED.to_string());
                    }
                    tokio::time::sleep(interval).await;
                }
                None => tokio::time::sleep(interval).await,
            }
        }
    }

    fn finished(&self, pod: &Value, using_eviction: bool, err: Option<&anyhow::Error>) {
        if let Some(f) = &self.on_pod_deletion_or_eviction_finished {
            f(pod, using_eviction, err);
        }
    }
}

/// `resource.EnhanceListError` (cli-runtime pkg/resource/helper.go).
fn enhance_list_error(
    err: anyhow::Error,
    label_selector: &str,
    field_selector: &str,
    subj: &str,
) -> anyhow::Error {
    if is_resource_expired(&err) {
        return err;
    }
    if is_bad_request(&err) || is_not_found(&err) {
        let text = if label_selector.is_empty() && field_selector.is_empty() {
            format!("Unable to list {}: {}", q(subj), err_message(&err))
        } else {
            format!(
                "Unable to find {} that match label selector {}, field selector {}: {}",
                q(subj),
                q(label_selector),
                q(field_selector),
                err_message(&err)
            )
        };
        // Retain the StatusError type, modify only the message.
        if let Some(se) = err.chain().find_map(|c| c.downcast_ref::<ApiStatusError>()) {
            let mut se = se.clone();
            se.message = format!(
                "Error from server{}: {}",
                if se.reason.is_empty() {
                    String::new()
                } else {
                    format!(" ({})", se.reason)
                },
                text
            );
            return anyhow::Error::new(se);
        }
        return anyhow!(text);
    }
    err
}

// ---------------------------------------------------------------------------
// cordon.go -- CordonHelper
// ---------------------------------------------------------------------------

/// `drain.CordonHelper` (kubectl/pkg/drain/cordon.go).
pub struct CordonHelper {
    node: Value,
    desired: bool,
}

impl CordonHelper {
    /// `NewCordonHelper`.
    pub fn new(node: Value) -> Self {
        Self {
            node,
            desired: false,
        }
    }

    /// `UpdateIfRequired` (cordon.go): true when `spec.unschedulable`
    /// differs from `desired`.
    pub fn update_if_required(&mut self, desired: bool) -> bool {
        self.desired = desired;
        let current = self
            .node
            .pointer("/spec/unschedulable")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        current != self.desired
    }

    /// `PatchOrReplace` (cordon.go): the strategic-merge patch
    /// `CreateTwoWayMergePatch(old, new)` yields for the one differing field.
    /// `unschedulable` is `omitempty`, so uncordoning deletes the key (`null`).
    pub async fn patch_or_replace(
        &mut self,
        client: &ApiClient,
        server_dry_run: bool,
    ) -> Result<()> {
        let name = self
            .node
            .pointer("/metadata/name")
            .and_then(|n| n.as_str())
            .unwrap_or("")
            .to_string();
        let patch = if self.desired {
            json!({"spec": {"unschedulable": true}})
        } else {
            json!({"spec": {"unschedulable": null}})
        };
        let mut path = format!("/api/v1/nodes/{name}");
        if server_dry_run {
            path.push_str("?dryRun=All");
        }
        let updated: Value = client
            .patch(&path, &patch, "application/strategic-merge-patch+json")
            .await?;
        let unschedulable = updated
            .pointer("/spec/unschedulable")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        self.node["spec"]["unschedulable"] = json!(unschedulable);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// cmd/drain/drain.go -- the commands
// ---------------------------------------------------------------------------

/// Which of the three commands is running (selects `cmd.Use` and the run func).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    Cordon,
    Uncordon,
    Drain,
}

impl Mode {
    fn use_line(self) -> &'static str {
        match self {
            Mode::Cordon => "cordon NODE",
            Mode::Uncordon => "uncordon NODE",
            Mode::Drain => "drain NODE",
        }
    }
    fn command_path(self) -> &'static str {
        match self {
            Mode::Cordon => "kubectl cordon",
            Mode::Uncordon => "kubectl uncordon",
            Mode::Drain => "kubectl drain",
        }
    }
}

/// `cmdutil.UsageErrorf` (helpers.go).
fn usage_errorf(mode: Mode, msg: &str) -> anyhow::Error {
    anyhow!(
        "{msg}\nSee '{} -h' for help and examples",
        mode.command_path()
    )
}

/// Parsed command-line flags of cordon/uncordon/drain.
#[derive(Clone, Debug)]
pub struct DrainFlags {
    pub force: bool,
    pub ignore_daemonsets: bool,
    pub delete_emptydir_data: bool,
    pub grace_period: i64,
    pub timeout: Duration,
    pub pod_selector: String,
    pub disable_eviction: bool,
    pub skip_wait_for_delete_timeout: i64,
    pub chunk_size: i64,
    /// `--dry-run` value (`none` default, `unchanged` for a bare flag).
    pub dry_run: String,
    /// `--selector`/`-l`; `None` when the flag was not passed.
    pub selector: Option<String>,
}

impl Default for DrainFlags {
    fn default() -> Self {
        Self {
            force: false,
            ignore_daemonsets: false,
            delete_emptydir_data: false,
            grace_period: -1,
            timeout: Duration::ZERO,
            pod_selector: String::new(),
            disable_eviction: false,
            skip_wait_for_delete_timeout: 0,
            chunk_size: 500,
            dry_run: "none".to_string(),
            selector: None,
        }
    }
}

/// `resource.Info` for a node: name and object.
pub struct NodeInfo {
    pub name: String,
    pub object: Value,
}

/// `printers.WarningPrinter` (cli-runtime pkg/printers/warningprinter.go).
struct WarningPrinter {
    out: Sink,
    color: bool,
}

impl WarningPrinter {
    fn print(&self, message: &str) {
        if self.color {
            (self.out)(&format!("\u{1b}[33;1mWarning:\u{1b}[0m {message}\n"));
        } else {
            (self.out)(&format!("Warning: {message}\n"));
        }
    }
}

/// `printers.AllowsColorOutput` for stderr.
fn allows_color_output() -> bool {
    std::io::stderr().is_terminal()
        && std::env::var("TERM").as_deref() != Ok("dumb")
        && std::env::var_os("NO_COLOR").is_none()
}

/// `name.go` `printObj`: `kind[.group]/name operation`.
fn name_print(
    obj: &Value,
    operation: &str,
    strategy: DryRunStrategy,
    w: &Sink,
) -> std::result::Result<(), String> {
    let kind = obj.get("kind").and_then(|k| k.as_str()).unwrap_or("");
    let api_version = obj.get("apiVersion").and_then(|k| k.as_str()).unwrap_or("");
    if kind.is_empty() && api_version.is_empty() {
        return Err(
            "missing apiVersion or kind; try GetObjectKind().SetGroupVersionKind() if you know the type"
                .to_string(),
        );
    }
    let name = obj
        .pointer("/metadata/name")
        .and_then(|n| n.as_str())
        .filter(|n| !n.is_empty())
        .unwrap_or("<unknown>");
    if kind.is_empty() {
        return Err(format!("missing kind for resource with name {name}"));
    }
    let group = api_version.split_once('/').map(|(g, _)| g).unwrap_or("");
    let operation = strategy.decorate(operation);
    let operation = if operation.is_empty() {
        operation
    } else {
        format!(" {operation}")
    };
    if group.is_empty() {
        w(&format!("{}/{name}{operation}\n", kind.to_lowercase()));
    } else {
        w(&format!(
            "{}.{group}/{name}{operation}\n",
            kind.to_lowercase()
        ));
    }
    Ok(())
}

/// `DrainCmdOptions` (cmd/drain/drain.go).
pub struct DrainCmdOptions<'a> {
    mode: Mode,
    pub drainer: Helper<'a>,
    pub node_infos: Vec<NodeInfo>,
    streams: IoStreams,
    warning_printer: Option<WarningPrinter>,
}

impl<'a> DrainCmdOptions<'a> {
    /// `NewDrainCmdOptions` (drain.go).
    pub fn new(mode: Mode, client: &'a ApiClient, streams: IoStreams) -> Self {
        Self {
            mode,
            drainer: Helper::new(client, &streams),
            node_infos: Vec::new(),
            streams,
            warning_printer: None,
        }
    }

    /// `o.ToPrinter(operation)` followed by `printObj(obj, o.Out)`.
    fn print_obj(&self, obj: &Value, operation: &str) -> std::result::Result<(), String> {
        name_print(
            obj,
            operation,
            self.drainer.dry_run_strategy,
            &self.streams.out,
        )
    }

    /// Install the `onPodDeletionOrEvictionFinished` printer (drain.go);
    /// done once the dry-run strategy is known (`Complete`).
    fn install_callbacks(&mut self) {
        let strategy = self.drainer.dry_run_strategy;
        let out = self.streams.out.clone();
        let err_out = self.streams.err_out.clone();
        self.drainer.on_pod_deletion_or_eviction_finished = Some(Arc::new(
            move |pod: &Value, using_eviction: bool, err: Option<&anyhow::Error>| {
                let verb = match (using_eviction, err.is_some()) {
                    (true, true) => "eviction failed",
                    (true, false) => "evicted",
                    (false, true) => "deletion failed",
                    (false, false) => "deleted",
                };
                if let Err(e) = name_print(pod, verb, strategy, &out) {
                    err_out(&format!("error building printer: {e}\n"));
                    out(&format!(
                        "pod {}/{} {verb}\n",
                        pod_namespace(pod),
                        pod_name(pod)
                    ));
                }
            },
        ));
    }

    /// `Complete` (drain.go): validate arguments and resolve the nodes.
    pub async fn complete(&mut self, args: &[String], flags: &DrainFlags) -> Result<()> {
        self.drainer.force = flags.force;
        self.drainer.ignore_all_daemon_sets = flags.ignore_daemonsets;
        self.drainer.delete_empty_dir_data = flags.delete_emptydir_data;
        self.drainer.grace_period_seconds = flags.grace_period;
        self.drainer.timeout = flags.timeout;
        self.drainer.pod_selector = flags.pod_selector.clone();
        self.drainer.disable_eviction = flags.disable_eviction;
        self.drainer.skip_wait_for_delete_timeout_seconds = flags.skip_wait_for_delete_timeout;
        self.drainer.chunk_size = flags.chunk_size;
        self.drainer.selector = flags.selector.clone().unwrap_or_default();

        if args.is_empty() && flags.selector.is_none() {
            return Err(usage_errorf(
                self.mode,
                &format!("USAGE: {} [flags]", self.mode.use_line()),
            ));
        }
        if !args.is_empty() && !self.drainer.selector.is_empty() {
            return Err(usage_errorf(
                self.mode,
                "error: cannot specify both a node name and a --selector option",
            ));
        }

        self.drainer.dry_run_strategy = DryRunStrategy::parse(&flags.dry_run)?;

        if !self.drainer.pod_selector.is_empty()
            && LabelSelector::parse(&self.drainer.pod_selector).is_err()
        {
            return Err(anyhow!(
                "--pod-selector=<pod_selector> must be a valid label selector"
            ));
        }

        self.node_infos = Vec::new();
        self.install_callbacks();
        // Set default WarningPrinter if not already set.
        if self.warning_printer.is_none() {
            self.warning_printer = Some(WarningPrinter {
                out: self.streams.err_out.clone(),
                color: allows_color_output(),
            });
        }

        self.node_infos = self.resolve_nodes(args).await?;
        Ok(())
    }

    /// The `f.NewBuilder()...ResourceNames("nodes", args...)` visit plus the
    /// "expected resource of type node" check.
    async fn resolve_nodes(&self, args: &[String]) -> Result<Vec<NodeInfo>> {
        let client = self.drainer.client;
        let mut infos = Vec::new();
        if self.drainer.selector.is_empty() {
            for arg in args {
                let name = match arg.split_once('/') {
                    Some((resource, name)) => {
                        if !matches!(resource, "node" | "nodes" | "no") {
                            return Err(anyhow!(
                                "error: expected resource of type node, got {}",
                                q(resource)
                            ));
                        }
                        name
                    }
                    None => arg.as_str(),
                };
                match client.get::<Value>(&format!("/api/v1/nodes/{name}")).await {
                    Ok(obj) => infos.push(node_info(obj)),
                    // The builder surfaces the server's StatusError as is.
                    Err(GetError::NotFound(status)) => return Err(anyhow::Error::new(status)),
                    Err(GetError::Other(e)) => return Err(e),
                }
            }
            return Ok(infos);
        }

        // LabelSelectorParam(selector).ResourceTypes("nodes"), chunked.
        let mut continue_token = String::new();
        loop {
            let mut query = url::form_urlencoded::Serializer::new(String::new());
            query.append_pair("labelSelector", &self.drainer.selector);
            if self.drainer.chunk_size > 0 {
                query.append_pair("limit", &self.drainer.chunk_size.to_string());
            }
            if !continue_token.is_empty() {
                query.append_pair("continue", &continue_token);
            }
            let page: Value = client
                .get(&format!("/api/v1/nodes?{}", query.finish()))
                .await
                .map_err(|e| anyhow!("{e}"))
                .map_err(|e| {
                    enhance_list_error(e, &self.drainer.selector, "", "/v1, Resource=nodes")
                })?;
            if let Some(items) = page.get("items").and_then(|i| i.as_array()) {
                infos.extend(items.iter().cloned().map(node_info));
            }
            continue_token = page
                .pointer("/metadata/continue")
                .and_then(|c| c.as_str())
                .unwrap_or("")
                .to_string();
            if continue_token.is_empty() {
                return Ok(infos);
            }
        }
    }

    /// `RunDrain` (drain.go).
    pub async fn run_drain(&mut self) -> Result<()> {
        self.run_cordon_or_uncordon(true).await?;

        let mut drained_nodes: Vec<String> = Vec::new();
        let mut fatal: Vec<String> = Vec::new();
        let mut remaining_nodes: Vec<String> = Vec::new();

        for i in 0..self.node_infos.len() {
            let (name, object) = (
                self.node_infos[i].name.clone(),
                self.node_infos[i].object.clone(),
            );
            match self.delete_or_evict_pods_simple(&name).await {
                Ok(()) => {
                    drained_nodes.push(name.clone());
                    if let Err(e) = self.print_obj(&object, "drained") {
                        return Err(anyhow!(e));
                    }
                }
                Err(err) => {
                    (self.streams.err_out)(&format!(
                        "error: unable to drain node {} due to error: {}, continuing command...\n",
                        q(&name),
                        err
                    ));
                    if !drained_nodes.contains(&name) {
                        fatal.push(err.to_string());
                        remaining_nodes.push(name);
                    }
                }
            }
        }

        if !remaining_nodes.is_empty() {
            (self.streams.err_out)("There are pending nodes to be drained:\n");
            for node_name in &remaining_nodes {
                (self.streams.err_out)(&format!(" {node_name}\n"));
            }
        }

        match aggregate(&fatal) {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// `deleteOrEvictPodsSimple` (drain.go).
    async fn delete_or_evict_pods_simple(&self, node_name: &str) -> Result<()> {
        let (list, errs) = self.drainer.get_pods_for_deletion(node_name).await;
        if let Some(e) = aggregate(&errs) {
            return Err(e);
        }
        let list = list.unwrap_or_default();
        let warnings = list.warnings();
        if !warnings.is_empty() {
            if let Some(wp) = &self.warning_printer {
                wp.print(&warnings);
            }
        }
        if self.drainer.dry_run_strategy == DryRunStrategy::Client {
            for pod in list.pods() {
                (self.streams.out)(&format!(
                    "evicting pod {}/{} (dry run)\n",
                    pod_namespace(&pod),
                    pod_name(&pod)
                ));
            }
            return Ok(());
        }

        if let Err(err) = self.drainer.delete_or_evict_pods(&list.pods()).await {
            let (pending_list, new_errs) = self.drainer.get_pods_for_deletion(node_name).await;
            if let Some(pending_list) = pending_list {
                let pods = pending_list.pods();
                if !pods.is_empty() {
                    (self.streams.err_out)(&format!(
                        "There are pending pods in node {} when an error occurred: {}\n",
                        q(node_name),
                        err
                    ));
                    for pending_pod in &pods {
                        (self.streams.err_out)(&format!("pod/{}\n", pod_name(pending_pod)));
                    }
                }
            }
            if let Some(agg) = aggregate(&new_errs) {
                (self.streams.err_out)(&format!(
                    "Following errors occurred while getting the list of pods to delete:\n{agg}"
                ));
            }
            return Err(err);
        }
        Ok(())
    }

    /// `RunCordonOrUncordon` (drain.go). The `skipped` branch for
    /// non-Node kinds cannot be reached: `Complete` only admits nodes.
    pub async fn run_cordon_or_uncordon(&mut self, desired: bool) -> Result<()> {
        let cordon_or_uncordon = if desired { "cordon" } else { "uncordon" };

        for node_info in &self.node_infos {
            let print_error = |err: &dyn std::fmt::Display| {
                (self.streams.err_out)(&format!(
                    "error: unable to {cordon_or_uncordon} node {}: {err}\n",
                    q(&node_info.name)
                ));
            };

            let mut c = CordonHelper::new(node_info.object.clone());
            if !c.update_if_required(desired) {
                if let Err(e) = self.print_obj(
                    &node_info.object,
                    if desired {
                        "already cordoned"
                    } else {
                        "already uncordoned"
                    },
                ) {
                    (self.streams.err_out)(&format!("error: {e}\n"));
                }
            } else {
                if self.drainer.dry_run_strategy != DryRunStrategy::Client {
                    if let Err(err) = c
                        .patch_or_replace(
                            self.drainer.client,
                            self.drainer.dry_run_strategy == DryRunStrategy::Server,
                        )
                        .await
                    {
                        print_error(&err_message(&err));
                        continue;
                    }
                }
                if let Err(e) = self.print_obj(
                    &node_info.object,
                    if desired { "cordoned" } else { "uncordoned" },
                ) {
                    (self.streams.err_out)(&format!("{e}\n"));
                }
            }
        }
        Ok(())
    }
}

/// Builder-supplied GVK: every object visited here is a `v1` `Node`.
fn node_info(mut obj: Value) -> NodeInfo {
    if let Some(o) = obj.as_object_mut() {
        o.insert("kind".into(), json!("Node"));
        o.insert("apiVersion".into(), json!("v1"));
    }
    let name = obj
        .pointer("/metadata/name")
        .and_then(|n| n.as_str())
        .unwrap_or("")
        .to_string();
    NodeInfo { name, object: obj }
}

/// `kubectl cordon NODE` (`NewCmdCordon`, drain.go).
pub async fn execute_cordon(
    client: &ApiClient,
    nodes: &[String],
    flags: &DrainFlags,
) -> Result<()> {
    let mut o = DrainCmdOptions::new(Mode::Cordon, client, IoStreams::std());
    o.complete(nodes, flags).await?;
    o.run_cordon_or_uncordon(true).await
}

/// `kubectl uncordon NODE` (`NewCmdUncordon`, drain.go).
pub async fn execute_uncordon(
    client: &ApiClient,
    nodes: &[String],
    flags: &DrainFlags,
) -> Result<()> {
    let mut o = DrainCmdOptions::new(Mode::Uncordon, client, IoStreams::std());
    o.complete(nodes, flags).await?;
    o.run_cordon_or_uncordon(false).await
}

/// `kubectl drain NODE` (`NewCmdDrain`, drain.go).
pub async fn execute_drain(client: &ApiClient, nodes: &[String], flags: &DrainFlags) -> Result<()> {
    let mut o = DrainCmdOptions::new(Mode::Drain, client, IoStreams::std());
    o.complete(nodes, flags).await?;
    o.run_drain().await
}
