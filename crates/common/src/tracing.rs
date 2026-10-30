// Basic tracing initialization.
//
// Distributed tracing via OpenTelemetry (Jaeger/OTLP) was removed: it was unused
// by every binary, the `opentelemetry-*` crates pinned in `Cargo.toml` had drifted
// to incompatible major versions, and Kubernetes conformance does not require
// trace export.

use anyhow::Result;
use std::sync::OnceLock;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{reload, EnvFilter, Registry};

/// The reloadable filter installed by [`init_basic_tracing`] plus the filter
/// directive it started from, so verbosity 0 can restore it.
struct FilterControl {
    handle: reload::Handle<EnvFilter, Registry>,
    base: String,
}

static FILTER_CONTROL: OnceLock<FilterControl> = OnceLock::new();

/// Initialize the global tracing subscriber with stdout fmt output.
///
/// `RUST_LOG` overrides `log_level` when set. The filter is reloadable so the
/// `/debug/flags/v` handler can change verbosity at runtime ([`set_verbosity`]).
pub fn init_basic_tracing(service_name: &str, log_level: &str) -> Result<()> {
    let base = std::env::var("RUST_LOG")
        .ok()
        .filter(|v| EnvFilter::try_new(v).is_ok())
        .unwrap_or_else(|| log_level.to_string());
    let env_filter = EnvFilter::try_new(&base).unwrap_or_else(|_| EnvFilter::new(log_level));
    let (filter, handle) = reload::Layer::new(env_filter);

    tracing_subscriber::registry()
        .with(filter)
        .with(
            tracing_subscriber::fmt::layer()
                .with_target(true)
                .with_thread_ids(true)
                .with_line_number(true),
        )
        .try_init()?;
    let _ = FILTER_CONTROL.set(FilterControl { handle, base });

    tracing::info!("Initialized basic tracing for service '{}'", service_name);

    Ok(())
}

/// Map a klog-style verbosity (`/debug/flags/v`) onto the tracing filter.
///
/// Rust-specific: klog `-v` has no tracing counterpart. Convention used here:
/// 0 restores the configured filter, 1-3 `info`, 4-5 `debug`, 6+ `trace`
/// (klog's own convention is V(2) = useful steady-state info, V(4) = debug,
/// V(5)+ = trace-level).
pub fn verbosity_directive(v: u32, base: &str) -> String {
    match v {
        0 => base.to_string(),
        1..=3 => "info".to_string(),
        4..=5 => "debug".to_string(),
        _ => "trace".to_string(),
    }
}

/// Apply a klog-style verbosity to the live subscriber. Errors when tracing
/// was not initialised through [`init_basic_tracing`].
pub fn set_verbosity(v: u32) -> Result<()> {
    let control = FILTER_CONTROL
        .get()
        .ok_or_else(|| anyhow::anyhow!("logging is not initialised"))?;
    let filter = EnvFilter::try_new(verbosity_directive(v, &control.base))?;
    control.handle.reload(filter)?;
    Ok(())
}
