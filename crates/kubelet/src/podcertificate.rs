//! Port of the kubelet's PodCertificate manager,
//! `pkg/kubelet/podcertificate/podcertificatemanager.go` (release-1.35),
//! the producer behind the projected volume's `podCertificate` source
//! (`pkg/volume/projected/projected.go:357-398` calls
//! `GetPodCertificateCredentialBundle`).
//!
//! The mechanism is ported whole: the per-projection state machine
//! (`credStateInitial -> credStateWait -> credStateFresh -> credStateWaitRefresh`,
//! with `credStateDenied` / `credStateFailed` terminal, `:144-266`), the
//! workqueue redriven by PCR events and a jittered 1-minute refresh pass
//! (`:339-352`, `:699-706`), the 10-minute assume-deleted and
//! refresh-overdue thresholds (`:90`, `:94`), the 5-minute jitter on
//! `beginRefreshAt` and the abandon time (`:695-697`), key + proof-of-possession
//! generation for all six key types (`:882-944`), PKCS#8 private-key PEM and
//! certificate-chain cleaning (`:946-979`), the metric report (`:821-875`) and
//! the no-op manager for static / detached mode (`:981-999`).
//!
//! Deviations, all deliberate:
//!
//! * No informers. Upstream reads PodCertificateRequests and the Node through
//!   shared-informer listers (`pcrLister`, `nodeLister`); the kubelet here
//!   reads straight from storage on each step, as `clustertrustbundle.rs`
//!   does. A PCR "not found" therefore means not found on the server, but the
//!   `assumeDeletedThreshold` window is kept verbatim. The informer's
//!   `AddEventHandler` (`:293-306`) is replaced by a storage watch on
//!   `/registry/podcertificaterequests/` that redrives the same projections.
//! * `GenerateName: "req-"` (`:762`) is filled by the API server upstream;
//!   `storage.create` takes a final key, so the suffix is generated here with
//!   `SimpleNameGenerator`'s alphabet (`staging/src/k8s.io/apiserver/pkg/storage/names/generate.go`)
//!   and its 5-character length.
//! * The workqueue is a small dedup / dirty-while-processing queue with a
//!   per-item exponential rate limiter (`workqueue.DefaultTypedControllerRateLimiter`
//!   = max(5ms..1000s per-item backoff, 10qps/100-burst bucket)); the overall
//!   bucket is omitted, the per-item backoff is verbatim.
//! * Upstream emits the Denied/Failed events with
//!   `Eventf(pod, Warning, <condition type>, cond.Reason, eventMessage)`
//!   (`:500`, `:509`) — `cond.Reason` lands in the *format string* position, so
//!   the emitted message is `Sprintf(cond.Reason, eventMessage)`, i.e. garbage
//!   like `Foo%!(EXTRA string=...)`. We emit the evident intent: reason =
//!   condition type, message = `eventMessage`.

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use rand::Rng;
use rusternetes_common::resources::pod::PodCertificateProjection;
use rusternetes_common::resources::podcertificaterequest::{
    PodCertificateRequest, CONDITION_TYPE_DENIED, CONDITION_TYPE_FAILED, CONDITION_TYPE_ISSUED,
};
use rusternetes_common::resources::{EventType, Node, Pod, ServiceAccount};
use rusternetes_common::types::ObjectMeta;
use rusternetes_storage::{build_key, build_prefix, EventRecorder, Storage, WatchEvent};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;
use tokio::sync::{Mutex, Notify};
use tracing::{debug, error, info};

/// `assumeDeletedThreshold` (`podcertificatemanager.go:90`): after this long
/// (plus jitter), a PCR we created that is missing must have been deleted.
pub const ASSUME_DELETED_THRESHOLD: Duration = Duration::from_secs(10 * 60);

/// `refreshOverdueDuration` (`:94`): a certificate this long past its
/// `BeginRefreshAt` is overdue for refresh.
pub const REFRESH_OVERDUE_DURATION: Duration = Duration::from_secs(10 * 60);

const PCR_RESOURCE: &str = "podcertificaterequests";

fn chrono_dur(d: Duration) -> ChronoDuration {
    ChronoDuration::from_std(d).unwrap_or_else(|_| ChronoDuration::zero())
}

/// `clock.PassiveClock`, the only clock surface the manager reads.
pub trait Clock: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
}

/// The wall clock.
pub struct RealClock;

impl Clock for RealClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

/// `PodManager` (`:55-59`): the local wrapper over `pod.Manager`.
pub trait PodManager: Send + Sync {
    fn get_pod_by_uid(&self, uid: &str) -> Option<Pod>;
    fn get_pods(&self) -> Vec<Pod>;
}

/// `SignerAndState` (`:82-86`).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SignerAndState {
    pub signer_name: String,
    pub state: String,
}

/// `MetricReport` (`:78-80`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MetricReport {
    pub pod_certificate_states: BTreeMap<SignerAndState, usize>,
}

/// `Manager` (`:63-75`): what Kubelet and the volume host need to provide pod
/// certificate functionality.
#[async_trait]
pub trait Manager: Send + Sync {
    /// Called by Kubelet every time a new pod is assigned to the node.
    fn track_pod(&self, pod: &Pod);
    /// Called by Kubelet every time a pod is dropped from the node.
    fn forget_pod(&self, pod: &Pod);
    /// Called by the volume host to retrieve `(privKey, certChain)` for one
    /// pod certificate volume source.
    async fn get_pod_certificate_credential_bundle(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        volume_name: &str,
        source_index: usize,
    ) -> Result<(Vec<u8>, Vec<u8>)>;
    /// A snapshot of current pod certificate states for this manager.
    fn metric_report(&self) -> MetricReport;
}

/// `projectionKey` (`:132-138`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ProjectionKey {
    pub namespace: String,
    pub pod_name: String,
    pub pod_uid: String,
    pub volume_name: String,
    pub source_index: usize,
}

/// The projection states (`:144-266`).
///
/// ```text
///                         ┌─────────────────┐
///                         ▼                 │
/// fresh ────► wait ────► fresh ──────► waitrefresh
///               │                           │
///               ├──────► denied ◄───────────┤
///               │                           │
///               └──────► failed ◄───────────┘
/// ```
#[derive(Debug, Clone)]
enum CredState {
    Initial,
    Wait {
        private_key: Vec<u8>,
        pcr_name: String,
        pcr_abandon_at: DateTime<Utc>,
    },
    Denied {
        reason: String,
        message: String,
    },
    Failed {
        reason: String,
        message: String,
    },
    Fresh(Issued),
    WaitRefresh {
        issued: Issued,
        refresh_private_key: Vec<u8>,
        refresh_pcr_name: String,
        refresh_pcr_abandon_at: DateTime<Utc>,
    },
}

/// The fields shared by `credStateFresh` and `credStateWaitRefresh`.
#[derive(Debug, Clone)]
struct Issued {
    private_key: Vec<u8>,
    cert_chain: Vec<u8>,
    begin_refresh_at: DateTime<Utc>,
    not_after: DateTime<Utc>,
    event_emitted_for_overdue_for_refresh: bool,
    event_emitted_for_expiration: bool,
}

impl Issued {
    /// `credStateFresh.metricsState` / `credStateWaitRefresh.metricsState`
    /// (`:229-237`, `:258-266`).
    fn metrics_state(&self, now: DateTime<Utc>) -> &'static str {
        if now > self.not_after {
            return "expired";
        }
        if now > self.begin_refresh_at + chrono_dur(REFRESH_OVERDUE_DURATION) {
            return "overdue_for_refresh";
        }
        "fresh"
    }
}

impl CredState {
    /// `getCredBundle` (`:159`, `:166-168`, `:182-184`, `:195-197`, `:208-210`,
    /// `:225-227`, `:254-256`), error strings verbatim.
    fn get_cred_bundle(&self) -> Result<(Vec<u8>, Vec<u8>)> {
        match self {
            CredState::Initial | CredState::Wait { .. } => {
                Err(anyhow!("credential bundle is not issued yet"))
            }
            CredState::Denied { reason, message } => Err(anyhow!(
                "PodCertificateRequest was permanently denied: reason={reason:?} message={message:?}"
            )),
            CredState::Failed { reason, message } => Err(anyhow!(
                "PodCertificateRequest was permanently failed: reason={reason:?} message={message:?}"
            )),
            CredState::Fresh(i) | CredState::WaitRefresh { issued: i, .. } => {
                Ok((i.private_key.clone(), i.cert_chain.clone()))
            }
        }
    }

    /// `metricsState` (`:170-172`, `:186-188`, `:199-201`, `:212-214`, `:229-237`).
    fn metrics_state(&self, now: DateTime<Utc>) -> &'static str {
        match self {
            CredState::Initial | CredState::Wait { .. } => "not_yet_issued",
            CredState::Denied { .. } => "denied",
            CredState::Failed { .. } => "failed",
            CredState::Fresh(i) | CredState::WaitRefresh { issued: i, .. } => i.metrics_state(now),
        }
    }
}

/// `projectionRecord` (`:140-155`): the lock covers the whole state.
struct ProjectionRecord {
    cur_state: Mutex<CredState>,
}

// ---------------------------------------------------------------------------
// workqueue
// ---------------------------------------------------------------------------

/// `workqueue.DefaultTypedControllerRateLimiter`'s per-item half:
/// `NewTypedItemExponentialFailureRateLimiter(5*time.Millisecond, 1000*time.Second)`
/// (`client-go/util/workqueue/default_rate_limiters.go`).
fn item_backoff(failures: u32) -> Duration {
    let base = Duration::from_millis(5);
    let max = Duration::from_secs(1000);
    let exp = failures.min(40);
    base.checked_mul(2u32.saturating_pow(exp.min(31)))
        .map(|d| d.min(max))
        .unwrap_or(max)
}

#[derive(Default)]
struct QueueState {
    queue: VecDeque<ProjectionKey>,
    /// Everything that needs processing (queued, or re-add pending while processing).
    dirty: HashSet<ProjectionKey>,
    processing: HashSet<ProjectionKey>,
    failures: HashMap<ProjectionKey, u32>,
    shutdown: bool,
}

/// The `workqueue.TypedRateLimitingInterface[projectionKey]` surface used by
/// the manager: `Add`, `AddRateLimited`, `Get`, `Done`, `Forget`, `ShutDown`.
struct ProjectionQueue {
    state: StdMutex<QueueState>,
    notify: Notify,
}

impl ProjectionQueue {
    fn new() -> Self {
        Self {
            state: StdMutex::new(QueueState::default()),
            notify: Notify::new(),
        }
    }

    fn add(&self, key: ProjectionKey) {
        let mut s = self.state.lock().unwrap();
        if s.shutdown || s.dirty.contains(&key) {
            return;
        }
        s.dirty.insert(key.clone());
        if s.processing.contains(&key) {
            return;
        }
        s.queue.push_back(key);
        drop(s);
        self.notify.notify_one();
    }

    /// `Get`: waits for an item; `None` once shut down and drained.
    async fn get(&self) -> Option<ProjectionKey> {
        loop {
            let notified = self.notify.notified();
            {
                let mut s = self.state.lock().unwrap();
                if let Some(key) = s.queue.pop_front() {
                    s.dirty.remove(&key);
                    s.processing.insert(key.clone());
                    return Some(key);
                }
                if s.shutdown {
                    return None;
                }
            }
            notified.await;
        }
    }

    fn done(&self, key: &ProjectionKey) {
        let mut s = self.state.lock().unwrap();
        s.processing.remove(key);
        if s.dirty.contains(key) {
            s.queue.push_back(key.clone());
            drop(s);
            self.notify.notify_one();
        }
    }

    fn forget(&self, key: &ProjectionKey) {
        self.state.lock().unwrap().failures.remove(key);
    }

    fn add_rate_limited(self: &Arc<Self>, key: ProjectionKey) {
        let delay = {
            let mut s = self.state.lock().unwrap();
            let n = s.failures.entry(key.clone()).or_insert(0);
            let d = item_backoff(*n);
            *n += 1;
            d
        };
        let q = Arc::clone(self);
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            q.add(key);
        });
    }

    fn shut_down(&self) {
        self.state.lock().unwrap().shutdown = true;
        self.notify.notify_waiters();
        self.notify.notify_one();
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.state.lock().unwrap().queue.len()
    }
}

// ---------------------------------------------------------------------------
// IssuingManager
// ---------------------------------------------------------------------------

/// `IssuingManager` (`:96-130`), the main implementation of [`Manager`].
///
/// State is not preserved across restarts — if Kubelet or the node restarts,
/// all PodCertificateProjections are queued for immediate refresh (`:104-105`).
pub struct IssuingManager<S: Storage + ?Sized + 'static> {
    storage: Arc<S>,
    pod_manager: Arc<dyn PodManager>,
    recorder: Option<EventRecorder<S>>,
    projection_queue: Arc<ProjectionQueue>,
    node_name: String,
    clock: Arc<dyn Clock>,
    /// `lock` covers credStore.
    cred_store: StdMutex<HashMap<ProjectionKey, Arc<ProjectionRecord>>>,
}

impl<S: Storage + ?Sized + 'static> IssuingManager<S> {
    /// `NewIssuingManager` (`:270-309`). `recorder` may be `None` (the upstream
    /// full-flow test passes `nil`); events are then skipped.
    pub fn new(
        storage: Arc<S>,
        pod_manager: Arc<dyn PodManager>,
        recorder: Option<EventRecorder<S>>,
        node_name: &str,
        clock: Arc<dyn Clock>,
    ) -> Arc<Self> {
        Arc::new(Self {
            storage,
            pod_manager,
            recorder,
            projection_queue: Arc::new(ProjectionQueue::new()),
            node_name: node_name.to_string(),
            clock,
            cred_store: StdMutex::new(HashMap::new()),
        })
    }

    /// `queueAllProjectionsForPod` (`:311-337`).
    pub fn queue_all_projections_for_pod(&self, uid: &str) {
        let Some(pod) = self.pod_manager.get_pod_by_uid(uid) else {
            return;
        };
        for key in projection_keys(&pod) {
            self.projection_queue.add(key.0);
        }
    }

    /// `Run` (`:339-352`): the refresh pass, the projection processor and the
    /// PCR event handler, until `cancel` resolves.
    pub async fn run(self: Arc<Self>, cancel: tokio_util_cancel::Token) {
        info!("podcertificate.IssuingManager starting up");

        // `wait.JitterUntilWithContext(ctx, m.runRefreshPass, 1*time.Minute, 1.0, false)`.
        let refresh = {
            let m = Arc::clone(&self);
            tokio::spawn(async move {
                loop {
                    m.run_refresh_pass();
                    let period = Duration::from_secs(60);
                    let jitter = period.mul_f64(rand::rng().random::<f64>());
                    tokio::time::sleep(period + jitter).await;
                }
            })
        };
        // `wait.UntilWithContext(ctx, m.runProjectionProcessor, time.Second)`.
        let processor = {
            let m = Arc::clone(&self);
            tokio::spawn(async move { m.run_projection_processor().await })
        };
        // Replaces the informer's `AddEventHandler` (`:293-306`).
        let watcher = {
            let m = Arc::clone(&self);
            tokio::spawn(async move { m.watch_pcrs().await })
        };

        cancel.cancelled().await;
        refresh.abort();
        watcher.abort();
        self.projection_queue.shut_down();
        let _ = processor.await;
        info!("podcertificate.IssuingManager shut down");
    }

    /// The PCR add/update/delete handlers (`:293-306`): redrive every
    /// projection of the PCR's pod.
    async fn watch_pcrs(self: Arc<Self>) {
        use futures::StreamExt;
        loop {
            match self.storage.watch(&build_prefix(PCR_RESOURCE, None)).await {
                Ok(mut stream) => {
                    while let Some(ev) = stream.next().await {
                        let value = match ev {
                            Ok(WatchEvent::Added(_, v))
                            | Ok(WatchEvent::Modified(_, v))
                            | Ok(WatchEvent::Deleted(_, v)) => v,
                            Err(_) => break,
                        };
                        if let Ok(pcr) = serde_json::from_str::<PodCertificateRequest>(&value) {
                            self.queue_all_projections_for_pod(&pcr.spec.pod_uid);
                        }
                    }
                }
                Err(e) => debug!("watching PodCertificateRequests: {e}"),
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    /// `runProjectionProcessor` (`:354-357`).
    async fn run_projection_processor(self: Arc<Self>) {
        while self.process_next_projection().await {}
    }

    /// `processNextProjection` (`:359-375`).
    async fn process_next_projection(self: &Arc<Self>) -> bool {
        let Some(key) = self.projection_queue.get().await else {
            return false;
        };
        let result = self.handle_projection(&key).await;
        self.projection_queue.done(&key);
        match result {
            Err(e) => {
                error!(
                    namespace = %key.namespace, pod = %key.pod_name, volume = %key.volume_name,
                    source_index = key.source_index,
                    "while handling podCertificate projected volume source: {e}"
                );
                self.projection_queue.add_rate_limited(key);
            }
            Ok(()) => self.projection_queue.forget(&key),
        }
        true
    }

    /// `handleProjection` (`:377-690`). Returning `Ok` drops the item from the
    /// queue; it is re-added by a PCR event or the refresh pass.
    pub async fn handle_projection(&self, key: &ProjectionKey) -> Result<()> {
        let Some(pod) = self.pod_manager.get_pod_by_uid(&key.pod_uid) else {
            // The pod is gone: clear all state associated with it and return
            // nil so it is forgotten from the queue (`:381-389`).
            self.cleanup_cred_store_for_pod(&key.namespace, &key.pod_name, &key.pod_uid);
            return Ok(());
        };

        let Some(source) = find_source(&pod, &key.volume_name, key.source_index) else {
            // No amount of retrying will fix this problem (`:401-405`).
            error!(
                ?key,
                "pod does not contain the named podCertificate projected volume source"
            );
            return Ok(());
        };

        let rec = {
            let mut store = self.cred_store.lock().unwrap();
            Arc::clone(store.entry(key.clone()).or_insert_with(|| {
                Arc::new(ProjectionRecord {
                    cur_state: Mutex::new(CredState::Initial),
                })
            }))
        };

        // Lock the record for the remainder of the function (`:422-424`).
        let mut guard = rec.cur_state.lock().await;
        let now = self.clock.now();

        match guard.clone() {
            CredState::Initial => {
                // `:427-466`.
                let (priv_key, pcr) = self
                    .create_pcr_for(&pod, &source)
                    .await
                    .map_err(|e| anyhow!("while creating initial PodCertificateRequest: {e}"))?;
                let created = pcr.metadata.creation_timestamp.unwrap_or(now);
                *guard = CredState::Wait {
                    private_key: priv_key,
                    pcr_name: pcr.metadata.name.clone(),
                    pcr_abandon_at: created
                        + chrono_dur(ASSUME_DELETED_THRESHOLD)
                        + chrono_dur(jitter_duration()),
                };
                debug!(?key, pcr = %pcr.metadata.name,
                    "PodCertificateRequest created, moving to credStateWait");
                Ok(())
            }

            CredState::Wait {
                private_key,
                pcr_name,
                pcr_abandon_at,
            } => {
                // `:468-527`.
                let pcr = match self.get_pcr(&key.namespace, &pcr_name).await {
                    Ok(p) => p,
                    Err(rusternetes_common::Error::NotFound(_)) if now > pcr_abandon_at => {
                        // "Not Found" could be informer lag or a real delete;
                        // after 10 minutes assume the latter and start over
                        // (`:474-484`).
                        *guard = CredState::Initial;
                        return Err(anyhow!(
                            "PodCertificateRequest \"{}/{}\" appears to have been deleted",
                            key.namespace,
                            pcr_name
                        ));
                    }
                    Err(e) => {
                        return Err(anyhow!(
                            "while getting PodCertificateRequest \"{}/{}\": {e}",
                            key.namespace,
                            pcr_name
                        ))
                    }
                };
                if let Some(next) = self.terminal_state(&pod, &pcr).await {
                    *guard = match next {
                        Terminal::Issued(chain, begin, not_after) => CredState::Fresh(Issued {
                            private_key,
                            cert_chain: chain,
                            begin_refresh_at: begin,
                            not_after,
                            event_emitted_for_overdue_for_refresh: false,
                            event_emitted_for_expiration: false,
                        }),
                        Terminal::State(s) => s,
                    };
                    return Ok(());
                }
                // Still pending: drop from the queue; a PCR update redrives it.
                debug!(
                    ?key,
                    "PodCertificateRequest not in terminal state, remaining in credStateWait"
                );
                Ok(())
            }

            // Permanent error states for the pod (`:529-537`).
            CredState::Denied { .. } | CredState::Failed { .. } => Ok(()),

            CredState::Fresh(mut state) => {
                // `:539-606`.
                if now < state.begin_refresh_at {
                    return Ok(());
                }
                debug!(?key, "Time to refresh");

                self.emit_overdue_and_expired(&pod, &mut state, now).await;
                // `state` mutations (event flags) must persist even if the
                // refresh PCR creation below fails (Go mutates the pointer).
                *guard = CredState::Fresh(state.clone());

                let (priv_key, pcr) = self
                    .create_pcr_for(&pod, &source)
                    .await
                    .map_err(|e| anyhow!("while creating refresh PodCertificateRequest: {e}"))?;
                let created = pcr.metadata.creation_timestamp.unwrap_or(now);
                *guard = CredState::WaitRefresh {
                    issued: state,
                    refresh_private_key: priv_key,
                    refresh_pcr_name: pcr.metadata.name.clone(),
                    refresh_pcr_abandon_at: created
                        + chrono_dur(ASSUME_DELETED_THRESHOLD)
                        + chrono_dur(jitter_duration()),
                };
                debug!(?key, pcr = %pcr.metadata.name,
                    "PodCertificateRequest created, moving to credStateWaitRefresh");
                Ok(())
            }

            CredState::WaitRefresh {
                mut issued,
                refresh_private_key,
                refresh_pcr_name,
                refresh_pcr_abandon_at,
            } => {
                // `:608-687`.
                let pcr = match self.get_pcr(&key.namespace, &refresh_pcr_name).await {
                    Ok(p) => p,
                    Err(rusternetes_common::Error::NotFound(_)) if now > refresh_pcr_abandon_at => {
                        // Return to credStateFresh so we create a new PCR (`:611-629`).
                        *guard = CredState::Fresh(issued);
                        return Err(anyhow!(
                            "PodCertificateRequest appears to have been deleted"
                        ));
                    }
                    Err(e) => {
                        return Err(anyhow!(
                            "while getting PodCertificateRequest \"{}/{}\": {e}",
                            key.namespace,
                            refresh_pcr_name
                        ))
                    }
                };
                if let Some(next) = self.terminal_state(&pod, &pcr).await {
                    *guard = match next {
                        Terminal::Issued(chain, begin, not_after) => CredState::Fresh(Issued {
                            private_key: refresh_private_key,
                            cert_chain: chain,
                            begin_refresh_at: begin,
                            not_after,
                            event_emitted_for_overdue_for_refresh: false,
                            event_emitted_for_expiration: false,
                        }),
                        Terminal::State(s) => s,
                    };
                    return Ok(());
                }
                debug!(?key, "PodCertificateRequest not in terminal state, remaining in credStateWaitRefresh");
                self.emit_overdue_and_expired(&pod, &mut issued, now).await;
                *guard = CredState::WaitRefresh {
                    issued,
                    refresh_private_key,
                    refresh_pcr_name,
                    refresh_pcr_abandon_at,
                };
                Ok(())
            }
        }
    }

    /// The overdue / expired event pair repeated at `:550-562` and `:673-685`.
    async fn emit_overdue_and_expired(&self, pod: &Pod, state: &mut Issued, now: DateTime<Utc>) {
        // More than 10 minutes past BeginRefreshAt but not yet flagged (`:551`).
        if now > state.begin_refresh_at + chrono_dur(REFRESH_OVERDUE_DURATION)
            && !state.event_emitted_for_overdue_for_refresh
        {
            debug!("Refresh overdue");
            self.event(
                pod,
                EventType::Warning,
                "CertificateOverdueForRefresh",
                "PodCertificate refresh overdue",
            )
            .await;
            state.event_emitted_for_overdue_for_refresh = true;
        }
        // Past NotAfter but not yet flagged (`:558`).
        if now > state.not_after && !state.event_emitted_for_expiration {
            debug!("Certificates expired");
            self.event(
                pod,
                EventType::Warning,
                "CertificateExpired",
                "PodCertificate expired",
            )
            .await;
            state.event_emitted_for_expiration = true;
        }
    }

    async fn event(&self, pod: &Pod, ty: EventType, reason: &str, message: &str) {
        if let Some(rec) = &self.recorder {
            if let Err(e) =
                crate::events::emit_lifecycle_event(rec, pod, None, reason, ty, message).await
            {
                debug!("recording {reason} event: {e}");
            }
        }
    }

    /// The `for _, cond := range pcr.Status.Conditions` switch shared by
    /// `credStateWait` (`:491-521`) and `credStateWaitRefresh` (`:636-666`);
    /// conditions are examined in order and the first terminal one wins.
    async fn terminal_state(&self, pod: &Pod, pcr: &PodCertificateRequest) -> Option<Terminal> {
        let pcr_name = format!(
            "{}/{}",
            pcr.metadata.namespace.clone().unwrap_or_default(),
            pcr.metadata.name
        );
        for cond in &pcr.status.conditions {
            let reason = cond.reason.clone().unwrap_or_default();
            let message = cond.message.clone().unwrap_or_default();
            match cond.condition_type.as_str() {
                CONDITION_TYPE_DENIED => {
                    debug!(pcr = %pcr_name, "PodCertificateRequest denied, moving to credStateDenied");
                    let msg = format!(
                        "PodCertificateRequest {pcr_name} was denied, reason={reason:?}, message={message:?}"
                    );
                    self.event(pod, EventType::Warning, CONDITION_TYPE_DENIED, &msg)
                        .await;
                    return Some(Terminal::State(CredState::Denied { reason, message }));
                }
                CONDITION_TYPE_FAILED => {
                    debug!(pcr = %pcr_name, "PodCertificateRequest failed, moving to credStateFailed");
                    let msg = format!(
                        "PodCertificateRequest {pcr_name} failed, reason={reason:?}, message={message:?}"
                    );
                    self.event(pod, EventType::Warning, CONDITION_TYPE_FAILED, &msg)
                        .await;
                    return Some(Terminal::State(CredState::Failed { reason, message }));
                }
                CONDITION_TYPE_ISSUED => {
                    debug!(pcr = %pcr_name, "PodCertificateRequest issued, moving to credStateFresh");
                    // `BeginRefreshAt.Time.Add(jitterDuration())` (`:515`).
                    let begin = pcr.status.begin_refresh_at.unwrap_or_default()
                        + chrono_dur(jitter_duration());
                    return Some(Terminal::Issued(
                        clean_certificate_chain(pcr.status.certificate_chain.as_bytes()),
                        begin,
                        pcr.status.not_after.unwrap_or_default(),
                    ));
                }
                _ => {}
            }
        }
        None
    }

    async fn get_pcr(
        &self,
        namespace: &str,
        name: &str,
    ) -> rusternetes_common::Result<PodCertificateRequest> {
        self.storage
            .get(&build_key(PCR_RESOURCE, Some(namespace), name))
            .await
    }

    /// The `credStateInitial` / `credStateFresh` preamble (`:431-451`,
    /// `:564-584`): look up the ServiceAccount UID and the Node UID, then
    /// create the PCR.
    async fn create_pcr_for(
        &self,
        pod: &Pod,
        source: &PodCertificateProjection,
    ) -> Result<(Vec<u8>, PodCertificateRequest)> {
        let ns = pod.metadata.namespace.clone().unwrap_or_default();
        let sa_name = pod
            .spec
            .as_ref()
            .and_then(|s| s.service_account_name.clone())
            .unwrap_or_default();
        // We fetch the service account so we can know its UID. Ideally,
        // Kubelet would have a central component that tracks all service
        // accounts related to pods on the node using a single-item watch.
        let sa: ServiceAccount = self
            .storage
            .get(&build_key("serviceaccounts", Some(&ns), &sa_name))
            .await
            .map_err(|e| anyhow!("while fetching service account: {e}"))?;
        let node: Node = self
            .storage
            .get(&build_key("nodes", None, &self.node_name))
            .await
            .map_err(|e| anyhow!("while getting node object from local cache: {e}"))?;

        self.create_pod_certificate_request(
            &ns,
            &pod.metadata.name,
            &pod.metadata.uid,
            &sa_name,
            &sa.metadata.uid,
            &node.metadata.uid,
            source,
        )
        .await
    }

    /// `createPodCertificateRequest` (`:735-793`).
    #[allow(clippy::too_many_arguments)]
    async fn create_pod_certificate_request(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        service_account_name: &str,
        service_account_uid: &str,
        node_uid: &str,
        source: &PodCertificateProjection,
    ) -> Result<(Vec<u8>, PodCertificateRequest)> {
        let (key_pem, pkix_public_key, proof) =
            generate_key_and_proof(&source.key_type, pod_uid.as_bytes())
                .map_err(|e| anyhow!("while generating keypair: {e}"))?;

        let name = format!("req-{}", simple_name_suffix());
        let mut req = PodCertificateRequest {
            metadata: ObjectMeta {
                name,
                generate_name: Some("req-".to_string()),
                namespace: Some(namespace.to_string()),
                owner_references: Some(vec![rusternetes_common::types::OwnerReference {
                    api_version: "core/v1".to_string(),
                    kind: "Pod".to_string(),
                    name: pod_name.to_string(),
                    uid: pod_uid.to_string(),
                    controller: None,
                    block_owner_deletion: None,
                }]),
                ..Default::default()
            },
            ..Default::default()
        };
        req.spec.signer_name = source.signer_name.clone();
        req.spec.pod_name = pod_name.to_string();
        req.spec.pod_uid = pod_uid.to_string();
        req.spec.service_account_name = service_account_name.to_string();
        req.spec.service_account_uid = service_account_uid.to_string();
        req.spec.node_name = self.node_name.clone();
        req.spec.node_uid = node_uid.to_string();
        req.spec.max_expiration_seconds = source.max_expiration_seconds;
        req.spec.pkix_public_key = pkix_public_key;
        req.spec.proof_of_possession = proof;
        req.spec.unverified_user_annotations = source.user_annotations.clone();

        let key = build_key(PCR_RESOURCE, Some(namespace), &req.metadata.name);
        let created: PodCertificateRequest = self
            .storage
            .create(&key, &req)
            .await
            .map_err(|e| anyhow!("while creating on API: {e}"))?;
        Ok((key_pem, created))
    }

    /// `runRefreshPass` (`:699-706`): add every pod on the node back to the
    /// projection queue.
    pub fn run_refresh_pass(&self) {
        for pod in self.pod_manager.get_pods() {
            self.queue_all_projections_for_pod(&pod.metadata.uid);
        }
    }

    /// `cleanupCredStoreForPod` (`:714-724`).
    fn cleanup_cred_store_for_pod(&self, namespace: &str, pod_name: &str, pod_uid: &str) {
        self.cred_store.lock().unwrap().retain(|k, _| {
            !(k.namespace == namespace && k.pod_name == pod_name && k.pod_uid == pod_uid)
        });
    }

    #[cfg(test)]
    fn queued(&self) -> usize {
        self.projection_queue.len()
    }
}

enum Terminal {
    /// Issued: cleaned chain, jittered `beginRefreshAt`, `notAfter`.
    Issued(Vec<u8>, DateTime<Utc>, DateTime<Utc>),
    State(CredState),
}

#[async_trait]
impl<S: Storage + ?Sized + 'static> Manager for IssuingManager<S> {
    /// `TrackPod` (`:710-712`).
    fn track_pod(&self, pod: &Pod) {
        self.queue_all_projections_for_pod(&pod.metadata.uid);
    }

    /// `ForgetPod` (`:726-733`): immediately clean up credStore entries to
    /// prevent race conditions.
    fn forget_pod(&self, pod: &Pod) {
        self.cleanup_cred_store_for_pod(
            pod.metadata.namespace.as_deref().unwrap_or_default(),
            &pod.metadata.name,
            &pod.metadata.uid,
        );
    }

    /// `GetPodCertificateCredentialBundle` (`:795-819`).
    async fn get_pod_certificate_credential_bundle(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        volume_name: &str,
        source_index: usize,
    ) -> Result<(Vec<u8>, Vec<u8>)> {
        let key = ProjectionKey {
            namespace: namespace.to_string(),
            pod_name: pod_name.to_string(),
            pod_uid: pod_uid.to_string(),
            volume_name: volume_name.to_string(),
            source_index,
        };
        let rec = self.cred_store.lock().unwrap().get(&key).cloned();
        let Some(rec) = rec else {
            return Err(anyhow!("no credentials yet for key={key:?}"));
        };
        let state = rec.cur_state.lock().await;
        state.get_cred_bundle()
    }

    /// `MetricReport` (`:821-875`): iterate the pods' projected sources
    /// rather than credStore, so the source's SignerName is available.
    fn metric_report(&self) -> MetricReport {
        let mut report = MetricReport::default();
        let now = self.clock.now();
        for pod in self.pod_manager.get_pods() {
            for (key, source) in projection_keys(&pod) {
                let rec = self.cred_store.lock().unwrap().get(&key).cloned();
                let Some(rec) = rec else { continue };
                // `try_lock`: a record mid-API-call is skipped for this
                // report rather than blocking a metrics scrape.
                let Ok(state) = rec.cur_state.try_lock() else {
                    continue;
                };
                let k = SignerAndState {
                    signer_name: source.signer_name.clone(),
                    state: state.metrics_state(now).to_string(),
                };
                *report.pod_certificate_states.entry(k).or_insert(0) += 1;
            }
        }
        report
    }
}

/// Every `(projectionKey, source)` of a pod's projected volumes.
fn projection_keys(pod: &Pod) -> Vec<(ProjectionKey, PodCertificateProjection)> {
    let mut out = Vec::new();
    let Some(spec) = pod.spec.as_ref() else {
        return out;
    };
    for v in spec.volumes.iter().flatten() {
        let Some(projected) = v.projected.as_ref() else {
            continue;
        };
        for (i, src) in projected.sources.iter().flatten().enumerate() {
            let Some(pc) = src.pod_certificate.as_ref() else {
                continue;
            };
            out.push((
                ProjectionKey {
                    namespace: pod.metadata.namespace.clone().unwrap_or_default(),
                    pod_name: pod.metadata.name.clone(),
                    pod_uid: pod.metadata.uid.clone(),
                    volume_name: v.name.clone(),
                    source_index: i,
                },
                pc.clone(),
            ));
        }
    }
    out
}

fn find_source(pod: &Pod, volume: &str, index: usize) -> Option<PodCertificateProjection> {
    projection_keys(pod)
        .into_iter()
        .find(|(k, _)| k.volume_name == volume && k.source_index == index)
        .map(|(_, s)| s)
}

/// `jitterDuration` (`:692-697`): a 5-minute randomized jitter, to prevent
/// multiple projections from synchronizing their PCR creations.
fn jitter_duration() -> Duration {
    Duration::from_nanos(rand::rng().random_range(0..5 * 60 * 1_000_000_000u64))
}

/// `SimpleNameGenerator`'s random suffix (`names/generate.go`: `maxGeneratedNameLength`
/// 63, 5 random chars from `bcdfghjklmnpqrstvwxz2456789`).
fn simple_name_suffix() -> String {
    const ALPHANUMS: &[u8] = b"bcdfghjklmnpqrstvwxz2456789";
    let mut rng = rand::rng();
    (0..5)
        .map(|_| ALPHANUMS[rng.random_range(0..ALPHANUMS.len())] as char)
        .collect()
}

// ---------------------------------------------------------------------------
// key generation
// ---------------------------------------------------------------------------

/// `generateKeyAndProof` + `x509.MarshalPKIXPublicKey` + `pemEncodeKey`
/// (`:882-944`, `:749-757`, `:946-956`): returns
/// `(PKCS#8 "PRIVATE KEY" PEM, PKIX public key DER, proof of possession)`.
///
/// The proof signs `toBeSigned` (the pod UID): ECDSA and RSA-PSS sign
/// `sha256(toBeSigned)` (`hashBytes`), Ed25519 signs the raw bytes.
pub fn generate_key_and_proof(
    key_type: &str,
    to_be_signed: &[u8],
) -> Result<(Vec<u8>, Vec<u8>, Vec<u8>)> {
    use ed25519_dalek::pkcs8::{EncodePrivateKey, EncodePublicKey};
    use rand_core::OsRng;

    let digest = Sha256::digest(to_be_signed);
    let (priv_der, pub_der, sig): (Vec<u8>, Vec<u8>, Vec<u8>) = match key_type {
        "RSA3072" | "RSA4096" => {
            let bits = if key_type == "RSA3072" { 3072 } else { 4096 };
            let key = rsa::RsaPrivateKey::new(&mut OsRng, bits)
                .map_err(|e| anyhow!("while generating RSA {bits} key: {e}"))?;
            // `rsa.SignPSS(rand.Reader, key, SHA256, digest, nil)`: nil opts =
            // PSSSaltLengthAuto = the maximum salt, `emLen - hLen - 2`.
            let em_len = (rsa::traits::PublicKeyParts::n(&key).bits() - 1).div_ceil(8);
            let salt_len = em_len.saturating_sub(digest.len() + 2);
            let signer = rsa::pss::SigningKey::<Sha256>::new_with_salt_len(key.clone(), salt_len);
            let sig = rsa::signature::hazmat::RandomizedPrehashSigner::sign_prehash_with_rng(
                &signer, &mut OsRng, &digest,
            )
            .map_err(|e| anyhow!("while signing proof: {e}"))?;
            (
                key.to_pkcs8_der()?.as_bytes().to_vec(),
                rsa::RsaPublicKey::from(&key)
                    .to_public_key_der()?
                    .as_bytes()
                    .to_vec(),
                rsa::signature::SignatureEncoding::to_vec(&sig),
            )
        }
        "ECDSAP256" => {
            let key = p256::ecdsa::SigningKey::random(&mut OsRng);
            let sig: p256::ecdsa::Signature =
                p256::ecdsa::signature::hazmat::PrehashSigner::sign_prehash(&key, &digest)
                    .map_err(|e| anyhow!("while signing proof: {e}"))?;
            (
                key.to_pkcs8_der()?.as_bytes().to_vec(),
                key.verifying_key().to_public_key_der()?.as_bytes().to_vec(),
                sig.to_der().as_bytes().to_vec(),
            )
        }
        "ECDSAP384" => {
            let key = p384::ecdsa::SigningKey::random(&mut OsRng);
            let sig: p384::ecdsa::Signature =
                p384::ecdsa::signature::hazmat::PrehashSigner::sign_prehash(
                    &key,
                    &left_pad(&digest, 48),
                )
                .map_err(|e| anyhow!("while signing proof: {e}"))?;
            (
                key.to_pkcs8_der()?.as_bytes().to_vec(),
                key.verifying_key().to_public_key_der()?.as_bytes().to_vec(),
                sig.to_der().as_bytes().to_vec(),
            )
        }
        "ECDSAP521" => {
            // p521's `ecdsa::SigningKey` has no PKCS#8 codec; the
            // `elliptic_curve::SecretKey` it wraps does.
            let secret = p521::SecretKey::random(&mut OsRng);
            let key = p521::ecdsa::SigningKey::from_bytes(&secret.to_bytes())
                .map_err(|e| anyhow!("while generating ECDSA P521 key: {e}"))?;
            let sig: p521::ecdsa::Signature =
                p521::ecdsa::signature::hazmat::PrehashSigner::sign_prehash(
                    &key,
                    &left_pad(&digest, 66),
                )
                .map_err(|e| anyhow!("while signing proof: {e}"))?;
            (
                secret.to_pkcs8_der()?.as_bytes().to_vec(),
                secret.public_key().to_public_key_der()?.as_bytes().to_vec(),
                sig.to_der().as_bytes().to_vec(),
            )
        }
        "ED25519" => {
            let key = ed25519_dalek::SigningKey::generate(&mut OsRng);
            let sig = ed25519_dalek::Signer::sign(&key, to_be_signed);
            (
                key.to_pkcs8_der()?.as_bytes().to_vec(),
                key.verifying_key().to_public_key_der()?.as_bytes().to_vec(),
                sig.to_bytes().to_vec(),
            )
        }
        other => return Err(anyhow!("unknown key type {other:?}")),
    };

    // `pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", ...})`: LF endings.
    let key_pem = pem::encode_config(
        &pem::Pem::new("PRIVATE KEY", priv_der),
        pem::EncodeConfig::new().set_line_ending(pem::LineEnding::LF),
    )
    .into_bytes();
    Ok((key_pem, pub_der, sig))
}

/// Go's `ecdsa.SignASN1` converts a hash shorter than the curve order to an
/// integer as-is (`hashToInt`), which is the same integer as the digest
/// left-padded with zeros; RustCrypto's prehash signer instead insists on at
/// least half the field size, so pad explicitly (the verifier in
/// `common::validation::podcertificaterequest` does the same).
fn left_pad(digest: &[u8], len: usize) -> Vec<u8> {
    let mut out = vec![0u8; len.saturating_sub(digest.len())];
    out.extend_from_slice(digest);
    out
}

/// `cleanCertificateChain` (`:958-979`): drop all inter-block data and block
/// headers, re-emitting every PEM block as a bare `CERTIFICATE`.
pub fn clean_certificate_chain(input: &[u8]) -> Vec<u8> {
    let mut out = String::new();
    if let Ok(blocks) = pem::parse_many(input) {
        for b in blocks {
            out.push_str(&pem::encode_config(
                &pem::Pem::new("CERTIFICATE", b.into_contents()),
                pem::EncodeConfig::new().set_line_ending(pem::LineEnding::LF),
            ));
        }
    }
    out.into_bytes()
}

/// `NoOpManager` (`:981-999`), for static / detached Kubelet mode.
pub struct NoOpManager;

#[async_trait]
impl Manager for NoOpManager {
    fn track_pod(&self, _pod: &Pod) {}
    fn forget_pod(&self, _pod: &Pod) {}
    async fn get_pod_certificate_credential_bundle(
        &self,
        _namespace: &str,
        _pod_name: &str,
        _pod_uid: &str,
        _volume_name: &str,
        _source_index: usize,
    ) -> Result<(Vec<u8>, Vec<u8>)> {
        Err(anyhow!("unimplemented"))
    }
    fn metric_report(&self) -> MetricReport {
        MetricReport::default()
    }
}

/// A minimal cancellation token (the crate has no `tokio-util`).
pub mod tokio_util_cancel {
    use std::sync::Arc;
    use tokio::sync::Notify;

    #[derive(Clone, Default)]
    pub struct Token {
        inner: Arc<(Notify, std::sync::atomic::AtomicBool)>,
    }

    impl Token {
        pub fn new() -> Self {
            Self::default()
        }
        pub fn cancel(&self) {
            self.inner
                .1
                .store(true, std::sync::atomic::Ordering::SeqCst);
            self.inner.0.notify_waiters();
        }
        pub async fn cancelled(&self) {
            loop {
                let n = self.inner.0.notified();
                if self.inner.1.load(std::sync::atomic::Ordering::SeqCst) {
                    return;
                }
                n.await;
            }
        }
    }
}

pub mod wiring;

#[cfg(test)]
mod tests;
