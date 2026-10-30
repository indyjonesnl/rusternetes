//! Kubelet event emission.
//!
//! Upstream, the kubelet is the source of truth for per-container lifecycle
//! events — `Pulling`, `Pulled`, `Failed` (image), `Created`, `Started`,
//! `Killing`, `BackOff`, `Unhealthy` (probe). Before this module the rusternetes
//! kubelet emitted none of these; the controller-manager's `EventsController`
//! *inferred* a subset of them by polling `pod.status`. With a unified
//! [`EventRecorder`] available, the kubelet now records the real events at the
//! actual transition points and the controller-manager stops inferring them.
//!
//! The reason strings here mirror `pkg/kubelet/events/event.go` exactly so the
//! events match what conformance and tooling (Lens, `kubectl describe`) expect.

use rusternetes_common::resources::{EventType, ObjectReference, Pod};
use rusternetes_storage::{EventRecorder, Storage};

// --- Reason strings, verbatim from pkg/kubelet/events/event.go ---

/// Image pull started.
pub const PULLING_IMAGE: &str = "Pulling";
/// Image pull succeeded.
pub const PULLED_IMAGE: &str = "Pulled";
/// Image pull failed (`FailedToPullImage`).
pub const FAILED_TO_PULL_IMAGE: &str = "Failed";
/// Container created.
pub const CREATED_CONTAINER: &str = "Created";
/// Container started.
pub const STARTED_CONTAINER: &str = "Started";
/// Container create/start failed (`FailedToCreateContainer` / `FailedToStartContainer`).
pub const FAILED_CONTAINER: &str = "Failed";
/// Container is being killed.
pub const KILLING_CONTAINER: &str = "Killing";
/// A postStart lifecycle hook failed; the container is killed
/// (`FailedPostStartHook`). Mirrors upstream `pkg/kubelet/events`.
pub const FAILED_POST_START_HOOK: &str = "FailedPostStartHook";
/// Container start is in back-off (`BackOffStartContainer`).
pub const BACK_OFF_START_CONTAINER: &str = "BackOff";
/// A liveness/readiness/startup probe failed (`ContainerUnhealthy`).
pub const CONTAINER_UNHEALTHY: &str = "Unhealthy";
/// A probe succeeded with warning (`ContainerProbeWarning`).
pub const CONTAINER_PROBE_WARNING: &str = "ProbeWarning";

/// A node filesystem resize failed (`FileSystemResizeFailed`,
/// `pkg/kubelet/events/event.go:67`).
pub const FILE_SYSTEM_RESIZE_FAILED: &str = "FileSystemResizeFailed";
/// A volume resize failed (`VolumeResizeFailed`,
/// `pkg/kubelet/events/event.go:65`).
pub const VOLUME_RESIZE_FAILED: &str = "VolumeResizeFailed";
/// A node filesystem resize succeeded (`FileSystemResizeSuccess`,
/// `pkg/kubelet/events/event.go:69`; the reason string is
/// `FileSystemResizeSuccessful`).
pub const FILE_SYSTEM_RESIZE_SUCCESS: &str = "FileSystemResizeSuccessful";

/// The component name the kubelet stamps on every event it sources.
pub const KUBELET_COMPONENT: &str = "kubelet";

/// Build the `involvedObject` reference for a pod-scoped event.
pub fn pod_object_reference(pod: &Pod) -> ObjectReference {
    let namespace = pod.metadata.namespace.clone().unwrap_or_default();
    ObjectReference {
        kind: Some("Pod".to_string()),
        namespace: Some(namespace),
        name: Some(pod.metadata.name.clone()),
        uid: Some(pod.metadata.uid.clone()),
        api_version: Some("v1".to_string()),
        resource_version: pod.metadata.resource_version.clone(),
        field_path: None,
    }
}

/// `spec.containers{<name>}` — the upstream `fieldPath` form that pins a
/// container-scoped event to a specific container in `kubectl describe`.
pub fn container_field_path(container_name: &str) -> String {
    format!("spec.containers{{{}}}", container_name)
}

/// Build the `involvedObject` reference for a container-scoped event: the pod
/// reference with `fieldPath` set to the container.
pub fn container_object_reference(pod: &Pod, container_name: &str) -> ObjectReference {
    let mut object_ref = pod_object_reference(pod);
    object_ref.field_path = Some(container_field_path(container_name));
    object_ref
}

/// Emit a kubelet lifecycle event through the shared recorder.
///
/// `container_name = Some(name)` scopes the event to a container (sets
/// `fieldPath`); `None` scopes it to the pod. The recorder applies the
/// correlator (spam-filter + aggregation + count) before persisting.
///
/// Taking the recorder explicitly (rather than reaching through a
/// `ContainerRuntime`, which needs a live Docker connection) keeps this
/// unit-testable against an in-memory backend.
pub async fn emit_lifecycle_event<S: Storage + ?Sized>(
    recorder: &EventRecorder<S>,
    pod: &Pod,
    container_name: Option<&str>,
    reason: &str,
    event_type: EventType,
    message: &str,
) -> rusternetes_common::Result<()> {
    let involved = match container_name {
        Some(name) => container_object_reference(pod, name),
        None => pod_object_reference(pod),
    };
    let source = rusternetes_common::resources::EventSource {
        component: KUBELET_COMPONENT.to_string(),
        host: pod
            .spec
            .as_ref()
            .and_then(|s| s.node_name.clone())
            .filter(|n| !n.is_empty()),
    };
    recorder
        .event(&involved, &source, event_type, reason, message)
        .await
}
