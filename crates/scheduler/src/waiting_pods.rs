//! Pods waiting in the Permit phase.
//!
//! Port of `pkg/scheduler/framework/runtime/waiting_pods_map.go`
//! (`waitingPodsMap`, `waitingPod`) and the `fwk.WaitingPod` interface
//! (`staging/src/k8s.io/kube-scheduler/framework/interface.go:319-330`).
//!
//! A waiting pod holds one timer per pending Permit plugin. `allow` stops the
//! plugin's timer and, once no plugin is pending, signals success; `reject`
//! stops every timer and signals Unschedulable. The signal is a size-1
//! channel written with a non-blocking send, so allow/reject never block and
//! only the first signal that fits is kept (upstream `s chan *fwk.Status`).

use crate::fwk::{Code, Status};
use rusternetes_common::resources::Pod;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::AbortHandle;

/// `waitingPod` (waiting_pods_map.go:70-77).
pub struct WaitingPod {
    pod: Pod,
    /// `pendingPlugins`: plugin name -> its timeout timer.
    pending_plugins: Mutex<HashMap<String, AbortHandle>>,
    tx: mpsc::Sender<Status>,
    rx: tokio::sync::Mutex<mpsc::Receiver<Status>>,
}

impl std::fmt::Debug for WaitingPod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WaitingPod")
            .field("pod", &self.pod.metadata.name)
            .field("pending_plugins", &self.pending_plugins())
            .finish()
    }
}

impl WaitingPod {
    /// `newWaitingPod` (waiting_pods_map.go:82-110): one timer per plugin in
    /// `plugins_max_wait_time`; on expiry the pod is rejected with
    /// `rejected due to timeout after waiting <d> at plugin <name>`. Must run
    /// inside a tokio runtime.
    pub fn new(pod: Pod, plugins_max_wait_time: HashMap<String, Duration>) -> Arc<Self> {
        // "Allow() and Reject() calls are non-blocking. This property is
        // guaranteed by using non-blocking send to this channel. This channel
        // has a buffer of size 1 to ensure that non-blocking send will not be
        // ignored".
        let (tx, rx) = mpsc::channel(1);
        let wp = Arc::new(Self {
            pod,
            pending_plugins: Mutex::new(HashMap::new()),
            tx,
            rx: tokio::sync::Mutex::new(rx),
        });
        // "The time.AfterFunc calls wp.Reject which iterates through
        // pendingPlugins map. Acquire the lock here so that time.AfterFunc can
        // only execute after newWaitingPod finishes."
        let mut pending = wp.pending_plugins.lock().unwrap_or_else(|e| e.into_inner());
        for (plugin, wait_time) in plugins_max_wait_time {
            let timer_wp = Arc::clone(&wp);
            let timer_plugin = plugin.clone();
            let handle = tokio::spawn(async move {
                tokio::time::sleep(wait_time).await;
                let msg = format!(
                    "rejected due to timeout after waiting {} at plugin {}",
                    go_duration(wait_time),
                    timer_plugin
                );
                timer_wp.reject(&timer_plugin, &msg);
            });
            pending.insert(plugin, handle.abort_handle());
        }
        drop(pending);
        wp
    }

    /// `GetPod`.
    pub fn pod(&self) -> &Pod {
        &self.pod
    }

    /// `GetPendingPlugins`: the names of the Permit plugins still pending.
    pub fn pending_plugins(&self) -> Vec<String> {
        self.pending_plugins
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .keys()
            .cloned()
            .collect()
    }

    /// `Allow` (waiting_pods_map.go:126-146): the plugin allows the pod. The
    /// success signal is delivered only when this was the last pending plugin.
    pub fn allow(&self, plugin_name: &str) {
        let mut pending = self
            .pending_plugins
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if let Some(timer) = pending.remove(plugin_name) {
            timer.abort();
        }
        // "Only signal success status after all plugins have allowed"
        if !pending.is_empty() {
            return;
        }
        // Non-blocking send: a full channel (a signal is already queued) is a
        // no-op.
        let _ = self.tx.try_send(Status::success());
    }

    /// `Reject` (waiting_pods_map.go:149-163): the pod is unschedulable.
    pub fn reject(&self, plugin_name: &str, msg: &str) {
        let pending = self
            .pending_plugins
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        for timer in pending.values() {
            timer.abort();
        }
        let mut s = Status::new(Code::Unschedulable, vec![msg.to_string()]);
        s.set_plugin(plugin_name);
        let _ = self.tx.try_send(s);
    }

    /// The receive of `WaitOnPermit` (`s := <-waitingPod.s`).
    pub(crate) async fn recv(&self) -> Status {
        match self.rx.lock().await.recv().await {
            Some(s) => s,
            // The sender lives in `self`, so the channel cannot close while
            // we hold `&self`.
            None => Status::from_error("waiting pod signal channel closed"),
        }
    }
}

/// `waitingPodsMap` (waiting_pods_map.go:35-39): the pods waiting in Permit,
/// by UID.
#[derive(Debug, Default)]
pub struct WaitingPodsMap {
    pods: Mutex<HashMap<String, Arc<WaitingPod>>>,
}

impl WaitingPodsMap {
    /// `NewWaitingPodsMap`.
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, Arc<WaitingPod>>> {
        self.pods.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// `add`.
    pub fn add(&self, wp: Arc<WaitingPod>) {
        let uid = wp.pod().metadata.uid.clone();
        self.lock().insert(uid, wp);
    }

    /// `remove`.
    pub fn remove(&self, uid: &str) {
        self.lock().remove(uid);
    }

    /// `get`.
    pub fn get(&self, uid: &str) -> Option<Arc<WaitingPod>> {
        self.lock().get(uid).cloned()
    }

    /// `iterate`: calls `callback` for every waiting pod. The map is
    /// snapshotted first, so a callback may allow or reject without
    /// deadlocking on the map.
    pub fn iterate(&self, mut callback: impl FnMut(&WaitingPod)) {
        let pods: Vec<Arc<WaitingPod>> = self.lock().values().cloned().collect();
        for wp in pods {
            callback(&wp);
        }
    }
}

/// Go's `time.Duration.String()`, for the timeout message (`%v`).
fn go_duration(d: Duration) -> String {
    let total_ns = d.as_nanos();
    if total_ns == 0 {
        return "0s".to_string();
    }
    if total_ns < 1_000 {
        return format!("{total_ns}ns");
    }
    if total_ns < 1_000_000 {
        return trim_frac(total_ns as f64 / 1e3, "µs");
    }
    if total_ns < 1_000_000_000 {
        return trim_frac(total_ns as f64 / 1e6, "ms");
    }
    let secs_total = d.as_secs();
    let frac_ns = d.subsec_nanos();
    let (h, m, s) = (secs_total / 3600, secs_total % 3600 / 60, secs_total % 60);
    let mut sec = format!("{s}");
    if frac_ns != 0 {
        let f = format!("{frac_ns:09}");
        sec = format!("{s}.{}", f.trim_end_matches('0'));
    }
    if h > 0 {
        format!("{h}h{m}m{sec}s")
    } else if m > 0 {
        format!("{m}m{sec}s")
    } else {
        format!("{sec}s")
    }
}

fn trim_frac(v: f64, unit: &str) -> String {
    let s = format!("{v:.3}");
    let s = s.trim_end_matches('0').trim_end_matches('.');
    format!("{s}{unit}")
}
