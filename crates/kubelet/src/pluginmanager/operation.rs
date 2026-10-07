//! Port of `pkg/kubelet/pluginmanager/operationexecutor`
//! (`operation_executor.go`, `operation_generator.go`) and of the
//! `pkg/util/goroutinemap` it runs operations in.
//!
//! The registration handshake lives in [`register_plugin`]: dial the plugin's
//! socket, `GetInfo`, pick the handler by `PluginInfo.type`, `ValidatePlugin`,
//! add to the actual state, `RegisterPlugin`, then `NotifyRegistrationStatus`.

use super::cache::{ActualStateOfWorld, PluginHandler, PluginInfo};
use hyper_util::rt::TokioIo;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;

/// Generated from `proto/pluginregistration/v1/api.proto` (package
/// `pluginregistration`).
#[allow(clippy::result_large_err, clippy::large_enum_variant)]
pub mod registerapi {
    tonic::include_proto!("pluginregistration");
}
use registerapi::registration_client::RegistrationClient;
use registerapi::{InfoRequest, RegistrationStatus};

/// `dialTimeoutDuration` / `notifyTimeoutDuration` (`operation_generator.go:40-41`).
const DIAL_TIMEOUT: Duration = Duration::from_secs(10);
const NOTIFY_TIMEOUT: Duration = Duration::from_secs(5);
/// The `GetInfo` context is `context.WithTimeout(ctx, time.Second)` (`:84`).
const GET_INFO_TIMEOUT: Duration = Duration::from_secs(1);

/// `initialDurationBeforeRetry` / `maxDurationBeforeRetry`
/// (`goroutinemap/exponentialbackoff/exponential_backoff.go:31,37`).
const INITIAL_DURATION_BEFORE_RETRY: Duration = Duration::from_millis(500);
const MAX_DURATION_BEFORE_RETRY: Duration = Duration::from_secs(2 * 60 + 2);

/// Why `GoRoutineMap::run` refused to start an operation: upstream's
/// `alreadyExistsError` and `exponentialBackoffError`, both of which the
/// reconciler treats as expected and does not log.
#[derive(Debug, PartialEq, Eq)]
pub enum RunError {
    AlreadyExists,
    ExponentialBackoff,
}

#[derive(Clone, Default)]
struct Operation {
    pending: bool,
    duration_before_retry: Duration,
    last_error_time: Option<Instant>,
}

/// Port of `goRoutineMap` with `exponentialBackOffOnError = true`
/// (`goroutinemap.go`): at most one operation per name, and after a failed
/// operation the name is refused until a doubling backoff elapses.
#[derive(Clone, Default)]
pub struct GoRoutineMap {
    operations: Arc<Mutex<HashMap<String, Operation>>>,
}

impl GoRoutineMap {
    pub fn new() -> Self {
        Self::default()
    }

    /// `Run`: start `f` in the background unless one is pending
    /// (`AlreadyExists`) or the previous failure's backoff has not elapsed.
    pub fn run<F>(&self, name: &str, f: F) -> Result<(), RunError>
    where
        F: std::future::Future<Output = Result<(), String>> + Send + 'static,
    {
        let mut ops = self.operations.lock().unwrap();
        let existing = ops.get(name).cloned().unwrap_or_default();
        if ops.contains_key(name) {
            if existing.pending {
                return Err(RunError::AlreadyExists);
            }
            // SafeToRetry: refused while `since(lastErrorTime) <= durationBeforeRetry`.
            if existing
                .last_error_time
                .is_some_and(|t| t.elapsed() <= existing.duration_before_retry)
            {
                return Err(RunError::ExponentialBackoff);
            }
        }
        ops.insert(
            name.to_string(),
            Operation {
                pending: true,
                ..existing
            },
        );
        drop(ops);

        let map = self.clone();
        let name = name.to_string();
        tokio::spawn(async move {
            // A panic inside the operation is a failure of the operation
            // (`RecoverFromPanic(&err)`), not of the map.
            let result = match tokio::spawn(f).await {
                Ok(r) => r,
                Err(e) => Err(format!("operation panicked: {e}")),
            };
            map.operation_complete(&name, result);
        });
        Ok(())
    }

    /// `operationComplete`: success forgets the name; failure keeps it and
    /// doubles the backoff (`ExponentialBackoff.Update`).
    fn operation_complete(&self, name: &str, result: Result<(), String>) {
        let mut ops = self.operations.lock().unwrap();
        match result {
            Ok(()) => {
                ops.remove(name);
            }
            Err(err) => {
                tracing::error!(operation = name, "operation failed: {err}");
                let op = ops.entry(name.to_string()).or_default();
                op.duration_before_retry = if op.duration_before_retry.is_zero() {
                    INITIAL_DURATION_BEFORE_RETRY
                } else {
                    (op.duration_before_retry * 2).min(MAX_DURATION_BEFORE_RETRY)
                };
                op.last_error_time = Some(Instant::now());
                op.pending = false;
            }
        }
    }

    /// `IsOperationPending`.
    pub fn is_operation_pending(&self, name: &str) -> bool {
        self.operations
            .lock()
            .unwrap()
            .get(name)
            .is_some_and(|o| o.pending)
    }
}

/// Port of `OperationExecutor` (`operation_executor.go:36-70`).
pub struct OperationExecutor {
    pending_operations: GoRoutineMap,
}

impl Default for OperationExecutor {
    fn default() -> Self {
        Self::new()
    }
}

impl OperationExecutor {
    pub fn new() -> Self {
        Self {
            pending_operations: GoRoutineMap::new(),
        }
    }

    /// `RegisterPlugin`: keyed on the socket path so a register and an
    /// unregister of the same socket are never concurrent.
    pub fn register_plugin(
        &self,
        socket_path: &str,
        plugin_uuid: &str,
        handlers: HashMap<String, Arc<dyn PluginHandler>>,
        asw: Arc<ActualStateOfWorld>,
    ) -> Result<(), RunError> {
        let socket = socket_path.to_string();
        let uuid = plugin_uuid.to_string();
        self.pending_operations
            .run(socket_path, register_plugin(socket, uuid, handlers, asw))
    }

    /// `UnregisterPlugin`.
    pub fn unregister_plugin(
        &self,
        info: PluginInfo,
        asw: Arc<ActualStateOfWorld>,
    ) -> Result<(), RunError> {
        let key = info.socket_path.clone();
        self.pending_operations
            .run(&key, unregister_plugin(info, asw))
    }
}

/// `GenerateUnregisterPluginFunc` (`operation_generator.go:136-155`).
async fn unregister_plugin(info: PluginInfo, asw: Arc<ActualStateOfWorld>) -> Result<(), String> {
    let Some(handler) = info.handler.clone() else {
        return Err(format!(
            "UnregisterPlugin error -- failed to get plugin handler for {}",
            info.socket_path
        ));
    };
    // Removed from the actual state BEFORE calling the consumer's DeRegister so
    // that a register event arriving meanwhile is processed as a Register call
    // (`operation_generator.go:146-147`).
    asw.remove_plugin(&info.socket_path);
    handler.deregister_plugin(&info.name, &info.endpoint).await;
    tracing::debug!(plugin = %info.name, "DeRegisterPlugin called");
    Ok(())
}

/// `dial` (`operation_generator.go:178-201`): connect to the plugin's unix
/// socket, bounded by `timeout`. tonic 0.12 rides hyper 1.x, so the tokio
/// `UnixStream` is wrapped in `TokioIo` (as in `volume_plugins::csi_client`).
async fn dial(socket_path: &str, timeout: Duration) -> Result<Channel, String> {
    let path = socket_path.to_string();
    let endpoint = Endpoint::try_from("http://[::]:50051")
        .map_err(|e| format!("failed to dial socket {socket_path}, err: {e}"))?;
    let connect = endpoint.connect_with_connector(service_fn(move |_: Uri| {
        let path = path.clone();
        async move {
            let stream = tokio::net::UnixStream::connect(path).await?;
            Ok::<_, std::io::Error>(TokioIo::new(stream))
        }
    }));
    match tokio::time::timeout(timeout, connect).await {
        Ok(Ok(ch)) => Ok(ch),
        Ok(Err(e)) => Err(format!("failed to dial socket {socket_path}, err: {e}")),
        Err(_) => Err(format!(
            "failed to dial socket {socket_path}, err: context deadline exceeded"
        )),
    }
}

/// `notifyPlugin` (`operation_generator.go:157-176`): tell the plugin whether
/// registration succeeded. When `err_str` is non-empty the returned error is
/// that message (even if the notification itself went through).
async fn notify_plugin(
    client: &mut RegistrationClient<Channel>,
    registered: bool,
    err_str: &str,
) -> Result<(), String> {
    let mut req = tonic::Request::new(RegistrationStatus {
        plugin_registered: registered,
        error: err_str.to_string(),
    });
    req.set_timeout(NOTIFY_TIMEOUT);
    if let Err(e) = client.notify_registration_status(req).await {
        return Err(format!("{err_str}: {e}"));
    }
    if !err_str.is_empty() {
        return Err(err_str.to_string());
    }
    Ok(())
}

/// `GenerateRegisterPluginFunc` (`operation_generator.go:75-134`).
async fn register_plugin(
    socket_path: String,
    plugin_uuid: String,
    handlers: HashMap<String, Arc<dyn PluginHandler>>,
    asw: Arc<ActualStateOfWorld>,
) -> Result<(), String> {
    let channel = dial(&socket_path, DIAL_TIMEOUT).await.map_err(|e| {
        format!("RegisterPlugin error -- dial failed at socket {socket_path}, err: {e}")
    })?;
    let mut client = RegistrationClient::new(channel);

    let mut req = tonic::Request::new(InfoRequest {});
    req.set_timeout(GET_INFO_TIMEOUT);
    let info = client
        .get_info(req)
        .await
        .map_err(|e| {
            format!(
                "RegisterPlugin error -- failed to get plugin info using RPC GetInfo at socket {socket_path}, err: {e}"
            )
        })?
        .into_inner();

    let Some(handler) = handlers.get(&info.r#type).cloned() else {
        let msg = format!(
            "RegisterPlugin error -- no handler registered for plugin type: {} at socket {socket_path}",
            info.r#type
        );
        notify_plugin(&mut client, false, &msg).await.map_err(|e| {
            format!(
                "RegisterPlugin error -- failed to send error at socket {socket_path}, err: {e}"
            )
        })?;
        return Err(msg);
    };

    let endpoint = if info.endpoint.is_empty() {
        socket_path.clone()
    } else {
        info.endpoint.clone()
    };

    if let Err(e) = handler.validate_plugin(&info.name, &endpoint, &info.supported_versions) {
        notify_plugin(
            &mut client,
            false,
            &format!("RegisterPlugin error -- plugin validation failed with err: {e}"),
        )
        .await
        .map_err(|e| {
            format!(
                "RegisterPlugin error -- failed to send error at socket {socket_path}, err: {e}"
            )
        })?;
        return Err("RegisterPlugin error -- pluginHandler.ValidatePluginFunc failed".to_string());
    }

    // Added to the actual state BEFORE the consumer's RegisterPlugin so that a
    // delete event during registration is processed as a DeRegister call
    // (`operation_generator.go:112-113`).
    if let Err(e) = asw.add_plugin(PluginInfo {
        socket_path: socket_path.clone(),
        timestamp: std::time::SystemTime::now(),
        uuid: plugin_uuid,
        handler: Some(handler.clone()),
        name: info.name.clone(),
        endpoint: endpoint.clone(),
    }) {
        tracing::error!(path = %socket_path, "RegisterPlugin error -- failed to add plugin: {e}");
    }

    if let Err(e) = handler
        .register_plugin(&info.name, &endpoint, &info.supported_versions, None)
        .await
    {
        return notify_plugin(
            &mut client,
            false,
            &format!("RegisterPlugin error -- plugin registration failed with err: {e}"),
        )
        .await;
    }

    // Notify is called after register to guarantee that even if notify fails
    // Register will always be called after validate (`:127`).
    notify_plugin(&mut client, true, "").await.map_err(|e| {
        format!(
            "RegisterPlugin error -- failed to send registration status at socket {socket_path}, err: {e}"
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Notify;

    /// `TestOperationExecutor_RegisterPlugin_ConcurrentRegisterPlugin`: a second
    /// operation on the same name while the first runs is `AlreadyExists`.
    #[tokio::test]
    async fn concurrent_same_name_is_already_exists() {
        let map = GoRoutineMap::new();
        let gate = Arc::new(Notify::new());
        let g = gate.clone();
        map.run("a", async move {
            g.notified().await;
            Ok(())
        })
        .unwrap();
        assert!(map.is_operation_pending("a"));
        assert_eq!(map.run("a", async { Ok(()) }), Err(RunError::AlreadyExists));
        // A different name is independent.
        map.run("b", async { Ok(()) }).unwrap();
        gate.notify_one();
    }

    /// `..._SerialRegisterPlugin`: after success the name is forgotten and can
    /// run again immediately.
    #[tokio::test]
    async fn serial_after_success_runs_again() {
        let map = GoRoutineMap::new();
        let n = Arc::new(AtomicUsize::new(0));
        for _ in 0..3 {
            let n2 = n.clone();
            map.run("a", async move {
                n2.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
            .unwrap();
            while map.is_operation_pending("a") {
                tokio::task::yield_now().await;
            }
        }
        assert_eq!(n.load(Ordering::SeqCst), 3);
    }

    /// goroutinemap `TestExponentialBackoff`: a failure refuses the name until
    /// the (doubling) backoff elapses.
    #[tokio::test]
    async fn failure_backs_off() {
        let map = GoRoutineMap::new();
        map.run("a", async { Err("boom".to_string()) }).unwrap();
        while map.is_operation_pending("a") {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            map.run("a", async { Ok(()) }),
            Err(RunError::ExponentialBackoff)
        );
    }
}
