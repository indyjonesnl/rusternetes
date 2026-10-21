pub mod advanced;
pub mod data_plane;
pub mod diagnosis;
#[allow(dead_code)]
pub mod framework;
pub mod framework_runtime;
pub mod node_volume_limits;
#[allow(dead_code)]
pub mod plugins;
pub mod profile;
pub mod queue;
pub mod resource_events;
pub mod scheduler;
pub mod scheduler_config;
pub mod score;
pub mod volume_assume;
pub mod volume_bind;
pub mod volume_binding;

use rusternetes_storage::StorageBackend;
use std::sync::Arc;
use tracing::info;

/// Configuration for the scheduler component.
pub struct SchedulerConfig {
    pub interval: u64,
    /// Scheduling profiles by scheduler name (from `--scheduler-config`);
    /// a single `default-scheduler` profile by default.
    pub profiles: profile::Map,
}

/// Run the scheduler component.
///
/// This is the main entry point for embedding the scheduler in the all-in-one binary.
/// Runs the scheduling loop until the process is terminated.
pub async fn run(storage: Arc<StorageBackend>, config: SchedulerConfig) -> anyhow::Result<()> {
    info!("Starting Rusternetes Scheduler");

    let scheduler = Arc::new(scheduler::Scheduler::new_with_profiles(
        storage,
        config.interval,
        config.profiles,
    ));
    scheduler.run().await?;

    Ok(())
}

/// Run the scheduler as an api-server client, reading pod/node/priorityclass
/// state from informers and writing through the binding/status subresources
/// and events — no direct storage handle.
///
/// This is the in-process counterpart of the binary's `--api-server-url`
/// mode: the all-in-one binary calls this with an [`rusternetes_client::http::ApiClient`]
/// pointed at its embedded api-server over loopback, so DNS/scheduler all share
/// the same trust boundary (only the api-server touches storage).
pub async fn run_with_api(
    client: Arc<rusternetes_client::http::ApiClient>,
    config: SchedulerConfig,
) -> anyhow::Result<()> {
    info!("Starting Rusternetes Scheduler (API mode)");

    let backend = data_plane::ApiBackend::new(client);
    let scheduler = Arc::new(scheduler::Scheduler::new_api_with_profiles(
        backend,
        config.interval,
        config.profiles,
    ));
    scheduler.run().await?;

    Ok(())
}
