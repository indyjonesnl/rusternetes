use chrono::Utc;
use futures::StreamExt;
use rusternetes_common::resources::{Event, EventSource, EventType, ObjectReference, Pod};
use rusternetes_storage::{
    build_prefix, EventRecorder, Storage, WorkQueue, RECONCILE_ALL_SENTINEL,
};
use std::sync::Arc;
use tokio::time::{sleep, Duration};

/// EventsController creates events for pod lifecycle changes
pub struct EventsController<S: Storage> {
    storage: Arc<S>,
    /// Unified recorder: every emission runs through the shared
    /// [`EventCorrelator`](rusternetes_common::event_correlator::EventCorrelator)
    /// (spam-filter + aggregation + count) before reaching storage, replacing
    /// the hand-rolled create/bump this controller used to do inline.
    recorder: EventRecorder<S>,
    sync_interval: Duration,
}

impl<S: Storage + 'static> EventsController<S> {
    /// Minimum time that must elapse between two recorded occurrences of an
    /// otherwise-identical event before the polling reconciler re-emits it.
    /// Prevents per-resync inflation while still letting genuinely recurring
    /// events accumulate.
    const EVENT_DEDUP_MIN_INTERVAL: chrono::Duration = chrono::Duration::seconds(60);

    pub fn new(storage: Arc<S>, sync_interval_secs: u64) -> Self {
        Self {
            recorder: EventRecorder::new(Arc::clone(&storage)),
            storage,
            sync_interval: Duration::from_secs(sync_interval_secs),
        }
    }

    /// Watch-based run loop. Watches events as primary resource.
    /// Falls back to periodic resync every 30s.
    pub async fn run(self: Arc<Self>) {
        println!(
            "Starting Events controller (sync interval: {:?})",
            self.sync_interval
        );

        let queue = WorkQueue::new();

        // ONE worker (#2517): upstream has no Events controller; events are plain
        // objects written by client-go's EventBroadcaster, so there is no worker
        // count to port.
        let worker_queue = queue.clone();
        let worker_self = Arc::clone(&self);
        tokio::spawn(async move {
            worker_self.worker(worker_queue).await;
        });

        loop {
            queue.add(RECONCILE_ALL_SENTINEL.into()).await;

            let prefix = build_prefix("events", None);
            let watch_result = self.storage.watch(&prefix).await;
            let mut watch = match watch_result {
                Ok(w) => w,
                Err(e) => {
                    eprintln!("Failed to establish watch: {}, retrying", e);
                    sleep(self.sync_interval).await;
                    continue;
                }
            };

            let mut resync = tokio::time::interval(std::time::Duration::from_secs(30));
            resync.tick().await;

            let mut watch_broken = false;
            while !watch_broken {
                tokio::select! {
                    event = watch.next() => {
                        match event {
                            Some(Ok(_)) => {
                                queue.add(RECONCILE_ALL_SENTINEL.into()).await;
                            }
                            Some(Err(e)) => {
                                eprintln!("Watch error: {}, reconnecting", e);
                                watch_broken = true;
                            }
                            None => {
                                eprintln!("Watch stream ended, reconnecting");
                                watch_broken = true;
                            }
                        }
                    }
                    _ = resync.tick() => {
                        queue.add(RECONCILE_ALL_SENTINEL.into()).await;
                    }
                }
            }
        }
    }

    async fn worker(&self, queue: WorkQueue) {
        while let Some(key) = queue.get().await {
            let result = self.reconcile_all().await.map_err(|e| e.to_string());
            match result {
                Ok(()) => queue.forget(&key).await,
                Err(e) => {
                    eprintln!("reconcile_all error: {}", e);
                    queue.requeue_rate_limited(key.clone()).await;
                }
            }
            queue.done(&key).await;
        }
    }

    /// Reconcile all pods and create events for lifecycle changes
    pub async fn reconcile_all(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Get all pods
        let pods: Vec<Pod> = self.storage.list("/registry/pods/").await?;

        for pod in pods {
            if let Err(e) = self.reconcile_pod_events(&pod).await {
                eprintln!(
                    "Error reconciling events for pod {}: {}",
                    pod.metadata.name, e
                );
            }
        }

        // Clean up old events (older than 1 hour)
        self.cleanup_old_events().await?;

        Ok(())
    }

    /// Reconcile events for a single pod
    async fn reconcile_pod_events(&self, pod: &Pod) -> Result<(), Box<dyn std::error::Error>> {
        let pod_name = &pod.metadata.name;
        let namespace = pod.metadata.namespace.as_deref().unwrap_or("default");
        let pod_uid = &pod.metadata.uid;

        // Create object reference for the pod
        let pod_ref = ObjectReference {
            kind: Some("Pod".to_string()),
            namespace: Some(namespace.to_string()),
            name: Some(pod_name.to_string()),
            uid: Some(pod_uid.to_string()),
            api_version: Some("v1".to_string()),
            resource_version: pod.metadata.resource_version.clone(),
            field_path: None,
        };

        // Check pod phase and create appropriate events
        if let Some(status) = &pod.status {
            use rusternetes_common::types::Phase;

            match &status.phase {
                Some(Phase::Pending) => {
                    // The `Scheduled` event is now emitted by the scheduler
                    // itself via the unified EventRecorder at bind time (it is
                    // the source of truth, mirroring upstream's
                    // `recorder.Eventf` after bind). This controller no longer
                    // SYNTHESISES it from `pod.status` — doing so double-sourced
                    // the event and could only guess the message. See
                    // crates/scheduler/src/scheduler.rs::bind_pod_to_node.
                }
                Some(Phase::Running) => {
                    // Container-lifecycle events (Pulling/Pulled/Created/Started/
                    // BackOff) are now emitted by the kubelet at the real
                    // transition points via the unified EventRecorder — it is the
                    // source of truth, mirroring upstream. This controller no
                    // longer *infers* them from pod.status, which previously
                    // double-sourced events and could only guess at messages.
                    // See crates/kubelet/src/events.rs.
                }
                Some(Phase::Succeeded) => {
                    // Container Started events are kubelet-owned now; this
                    // controller only summarises the pod-level outcome the
                    // kubelet does not emit.
                    self.create_event_if_new(
                        namespace,
                        &pod_ref,
                        "Completed",
                        &format!("Pod {} completed successfully", pod_name),
                        EventType::Normal,
                        Some("kubelet".to_string()),
                    )
                    .await?;
                }
                Some(Phase::Failed) => {
                    // Container lifecycle events are kubelet-owned; keep only the
                    // pod-level Failed summary the kubelet does not emit.
                    let reason = status.reason.as_deref().unwrap_or("Unknown");
                    let message = status.message.as_deref().unwrap_or("Pod failed");
                    self.create_event_if_new(
                        namespace,
                        &pod_ref,
                        "Failed",
                        &format!("Pod {} failed: {} - {}", pod_name, reason, message),
                        EventType::Warning,
                        Some("kubelet".to_string()),
                    )
                    .await?;
                }
                Some(Phase::Unknown) => {}
                Some(Phase::Active) => {
                    // Active phase for namespaces - not relevant for pod events
                }
                Some(Phase::Terminating) => {
                    // Terminating phase for namespaces - not relevant for pod events
                }
                None => {
                    // Pod has no phase set
                }
            }
        }

        Ok(())
    }

    /// Create an event if it doesn't already exist (deduplicate)
    pub async fn create_event_if_new(
        &self,
        namespace: &str,
        involved_object: &ObjectReference,
        reason: &str,
        message: &str,
        event_type: EventType,
        component: Option<String>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Polling-reconciler guard: this loop re-derives pod-lifecycle state
        // from `pod.status` on every resync (~30s). Without a throttle we would
        // re-enter the recorder for an unchanged observation each pass, letting
        // the spam-filter's burst window bleed count onto a genuinely-static
        // event. Only proceed when the event is new, its message changed, or a
        // real interval has elapsed since the last recorded occurrence.
        let event_name = Event::generate_name(involved_object, reason);
        let key = format!("/registry/events/{}/{}", namespace, event_name);
        if let Ok(existing_event) = self.storage.get::<Event>(&key).await {
            let now = Utc::now();
            let message_changed = existing_event.message != message;
            let interval_elapsed = existing_event
                .last_timestamp
                .map(|t| now - t >= Self::EVENT_DEDUP_MIN_INTERVAL)
                .unwrap_or(true);

            if !message_changed && !interval_elapsed {
                // Too soon and nothing changed — treat as the same observation.
                return Ok(());
            }
        }

        // Delegate the actual create/bump to the unified recorder so the
        // spam-filter + aggregation + count stages run. The recorder is the
        // single authority on `count` and `series`.
        let source = EventSource {
            component: component.unwrap_or_else(|| "rusternetes".to_string()),
            host: None,
        };
        self.recorder
            .event(involved_object, &source, event_type, reason, message)
            .await
            .map_err(|e| -> Box<dyn std::error::Error> {
                format!(
                    "event recorder failed for {}/{}: {}",
                    namespace, event_name, e
                )
                .into()
            })?;

        Ok(())
    }

    /// Clean up events older than 1 hour
    async fn cleanup_old_events(&self) -> Result<(), Box<dyn std::error::Error>> {
        let all_events: Vec<Event> = self.storage.list("/registry/events/").await?;
        let one_hour_ago = Utc::now() - chrono::Duration::hours(1);

        for event in all_events {
            // Use last_timestamp, falling back to creation_timestamp
            let event_time = event.last_timestamp.or(event.metadata.creation_timestamp);
            if event_time.is_some_and(|t| t < one_hour_ago) {
                let namespace = event.metadata.namespace.as_deref().unwrap_or("default");
                let name = &event.metadata.name;
                let key = format!("/registry/events/{}/{}", namespace, name);

                if let Err(e) = self.storage.delete(&key).await {
                    eprintln!("Failed to delete old event {}: {}", name, e);
                }
            }
        }

        Ok(())
    }
}
