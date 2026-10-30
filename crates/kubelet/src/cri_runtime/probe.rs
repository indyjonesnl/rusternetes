//! Single-attempt container probing for the CRI runtime.
//!
//! The kubelet owns the probe *state machine* (initial delay, period, success/
//! failure thresholds) — that is runtime-agnostic. What is runtime-specific is
//! executing one probe attempt against a container, which is what
//! [`super::CriContainerRuntime::probe_container`] does: exec probes run via the
//! CRI `ExecSync`, while http/tcp probes are dialed from the node against the
//! pod IP. This module holds the pure helpers (port resolution).

use rusternetes_common::resources::pod::Container;
use rusternetes_common::resources::policy::IntOrString;

/// Resolve a probe port to a numeric container port. Integer ports pass through;
/// named ports are looked up in the container's declared `ports`. Returns `None`
/// if a named port is not declared.
pub fn resolve_port(container: &Container, port: &IntOrString) -> Option<i32> {
    match port {
        IntOrString::Int(n) => Some(*n),
        IntOrString::String(name) => container.ports.as_ref().and_then(|ports| {
            ports
                .iter()
                .find(|p| p.name.as_deref() == Some(name.as_str()))
                .map(|p| i32::from(p.container_port))
        }),
    }
}

/// Kubernetes HTTP probes follow redirects only while the target hostname stays
/// local to the original request. Redirects to a different hostname terminate
/// the probe with a warning result, which still counts as success for liveness.
pub fn redirect_is_non_local(next: &reqwest::Url, previous: &[reqwest::Url]) -> bool {
    previous
        .first()
        .is_some_and(|first| next.host_str() != first.host_str())
}
