//! Port of `pkg/volume/util/nestedpendingoperations/nestedpendingoperations.go`.
//!
//! A data structure for managing operations by volume/pod/node name: it refuses
//! a new operation while a matching one is pending (`AlreadyExists`), and after
//! a failure refuses a retry of the same operation until an exponential backoff
//! (`goroutinemap/exponentialbackoff`) elapses (`ExponentialBackoff`). Several
//! operations on one volume may run in parallel when they act on different pods.
//!
//! Upstream runs the operation in a goroutine ([`NestedPendingOperations::run`]
//! spawns a task). The kubelet's device-unmount sweep awaits its operation in
//! place because the generated operation borrows the sweep's state, so
//! [`NestedPendingOperations::run_inline`] drives the same bookkeeping on the
//! caller's task. Both share `start`/`operation_complete`.

use super::exponential_backoff::{ExponentialBackoff, ExponentialBackoffError};
use std::future::Future;
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

/// `EmptyUniquePodName` / `EmptyNodeName` (and `EmptyUniqueVolumeName`).
pub const EMPTY: &str = "";

/// `operationKey{volumeName, podName, nodeName}`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OperationKey {
    pub volume_name: String,
    pub pod_name: String,
    pub node_name: String,
}

impl OperationKey {
    pub fn new(volume_name: &str, pod_name: &str, node_name: &str) -> Self {
        Self {
            volume_name: volume_name.to_string(),
            pod_name: pod_name.to_string(),
            node_name: node_name.to_string(),
        }
    }
}

/// Why `Run` refused: `alreadyExistsError` or `exponentialBackoffError`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RunError {
    AlreadyExists(OperationKey),
    ExponentialBackoff(ExponentialBackoffError),
}

impl RunError {
    /// `IsAlreadyExists`.
    pub fn is_already_exists(&self) -> bool {
        matches!(self, RunError::AlreadyExists(_))
    }

    /// `exponentialbackoff.IsExponentialBackoff`.
    pub fn is_exponential_backoff(&self) -> bool {
        matches!(self, RunError::ExponentialBackoff(_))
    }
}

impl std::fmt::Display for RunError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RunError::AlreadyExists(key) => write!(
                f,
                "Failed to create operation with name {key:?}. An operation with that name is already executing."
            ),
            RunError::ExponentialBackoff(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for RunError {}

#[derive(Clone, Debug)]
struct Operation {
    key: OperationKey,
    operation_name: String,
    operation_pending: bool,
    exp_backoff: ExponentialBackoff,
}

#[derive(Default)]
struct Inner {
    operations: Vec<Operation>,
    exponential_back_off_on_error: bool,
}

/// `nestedPendingOperations`.
#[derive(Clone)]
pub struct NestedPendingOperations {
    inner: Arc<Mutex<Inner>>,
    /// `grm.cond`, for [`Self::wait`].
    changed: Arc<Notify>,
}

impl NestedPendingOperations {
    /// `NewNestedPendingOperations`.
    pub fn new(exponential_back_off_on_error: bool) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                operations: Vec::new(),
                exponential_back_off_on_error,
            })),
            changed: Arc::new(Notify::new()),
        }
    }

    /// `Run`: register the operation and spawn `operation` in the background.
    pub fn run<F>(
        &self,
        volume_name: &str,
        pod_name: &str,
        node_name: &str,
        operation_name: &str,
        operation: F,
    ) -> Result<(), RunError>
    where
        F: Future<Output = Result<(), String>> + Send + 'static,
    {
        let key = self.start(volume_name, pod_name, node_name, operation_name)?;
        let this = self.clone();
        tokio::spawn(async move {
            // `defer k8sRuntime.HandleCrash()`: a panic is a failed operation.
            let result = match tokio::spawn(operation).await {
                Ok(r) => r,
                Err(e) => Err(format!("operation panicked: {e}")),
            };
            this.operation_complete(&key, result);
        });
        Ok(())
    }

    /// [`Self::run`] awaiting the operation on the caller's task. The pending
    /// mark is cleared even if the caller is dropped mid-operation (as a
    /// failure, like a panicking goroutine).
    pub async fn run_inline<F>(
        &self,
        volume_name: &str,
        pod_name: &str,
        node_name: &str,
        operation_name: &str,
        operation: F,
    ) -> Result<Result<(), String>, RunError>
    where
        F: Future<Output = Result<(), String>>,
    {
        struct Guard<'a> {
            ops: &'a NestedPendingOperations,
            key: OperationKey,
            done: bool,
        }
        impl Drop for Guard<'_> {
            fn drop(&mut self) {
                if !self.done {
                    self.ops
                        .operation_complete(&self.key, Err("operation cancelled".to_string()));
                }
            }
        }
        let key = self.start(volume_name, pod_name, node_name, operation_name)?;
        let mut guard = Guard {
            ops: self,
            key,
            done: false,
        };
        let result = operation.await;
        guard.done = true;
        self.operation_complete(&guard.key, result.clone());
        Ok(result)
    }

    /// The admission half of `Run` (everything before `go func()`).
    fn start(
        &self,
        volume_name: &str,
        pod_name: &str,
        node_name: &str,
        operation_name: &str,
    ) -> Result<OperationKey, RunError> {
        let mut inner = self.inner.lock().unwrap();
        let op_key = OperationKey::new(volume_name, pod_name, node_name);
        match inner.is_operation_exists(&op_key) {
            Some(idx) => {
                let previous = &mut inner.operations[idx];
                if previous.operation_pending {
                    return Err(RunError::AlreadyExists(op_key));
                }
                if let Err(back_off_err) =
                    previous.exp_backoff.safe_to_retry(&format!("{op_key:?}"))
                {
                    if previous.operation_name == operation_name {
                        return Err(RunError::ExponentialBackoff(back_off_err));
                    }
                    // previous operation and new operation are different.
                    // reset op. name and exp. backoff
                    previous.operation_name = operation_name.to_string();
                    previous.exp_backoff = ExponentialBackoff::default();
                }
                previous.operation_pending = true;
                previous.key = op_key.clone();
            }
            None => inner.operations.push(Operation {
                key: op_key.clone(),
                operation_pending: true,
                operation_name: operation_name.to_string(),
                exp_backoff: ExponentialBackoff::default(),
            }),
        }
        Ok(op_key)
    }

    /// `IsOperationPending`.
    pub fn is_operation_pending(&self, volume_name: &str, pod_name: &str, node_name: &str) -> bool {
        let inner = self.inner.lock().unwrap();
        let key = OperationKey::new(volume_name, pod_name, node_name);
        inner
            .is_operation_exists(&key)
            .is_some_and(|i| inner.operations[i].operation_pending)
    }

    /// `IsOperationSafeToRetry`: false if a matching operation is pending or in
    /// exponential backoff (for the same operation name).
    pub fn is_operation_safe_to_retry(
        &self,
        volume_name: &str,
        pod_name: &str,
        node_name: &str,
        operation_name: &str,
    ) -> bool {
        let inner = self.inner.lock().unwrap();
        let key = OperationKey::new(volume_name, pod_name, node_name);
        let Some(idx) = inner.is_operation_exists(&key) else {
            return true;
        };
        let previous = &inner.operations[idx];
        if previous.operation_pending {
            return false;
        }
        !(previous
            .exp_backoff
            .safe_to_retry(&format!("{key:?}"))
            .is_err()
            && previous.operation_name == operation_name)
    }

    /// `operationComplete`.
    fn operation_complete(&self, key: &OperationKey, result: Result<(), String>) {
        {
            let mut inner = self.inner.lock().unwrap();
            let backoff_on = inner.exponential_back_off_on_error;
            match result {
                Ok(()) => inner.delete_operation(key),
                Err(err) if !backoff_on => {
                    inner.delete_operation(key);
                    tracing::error!("operation {key:?} failed with: {err}");
                }
                Err(err) => match inner.get_operation(key) {
                    None => tracing::error!(
                        "Operation {key:?} completed. error: {err}. exponentialBackOffOnError is enabled, but failed to get operation to update."
                    ),
                    Some(idx) => {
                        let op = &mut inner.operations[idx];
                        op.exp_backoff.update(&err);
                        op.operation_pending = false;
                        tracing::error!(
                            "{}",
                            op.exp_backoff.no_retries_permitted_msg(&format!("{key:?}"))
                        );
                    }
                },
            }
        }
        self.changed.notify_waiters();
    }

    /// `Wait`: block until every operation is gone. As upstream, an operation
    /// parked in backoff keeps its entry, so this waits for success.
    pub async fn wait(&self) {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.inner.lock().unwrap().operations.is_empty() {
                return;
            }
            notified.await;
        }
    }
}

impl Inner {
    /// `isOperationExists`: the index of the operation whose key matches `key`
    /// (a pending match wins; otherwise an exact match, else the first).
    fn is_operation_exists(&self, key: &OperationKey) -> Option<usize> {
        // If volumeName is empty, operation can be executed concurrently
        if key.volume_name == EMPTY {
            return None;
        }
        let mut op_index = None;
        for (i, prev) in self.operations.iter().enumerate() {
            let volume_name_match = prev.key.volume_name == key.volume_name;
            let pod_name_match = prev.key.pod_name == EMPTY
                || key.pod_name == EMPTY
                || prev.key.pod_name == key.pod_name;
            let pod_name_exact_match = prev.key.pod_name == key.pod_name;
            let node_name_match = prev.key.node_name == EMPTY
                || key.node_name == EMPTY
                || prev.key.node_name == key.node_name;
            let node_name_exact_match = prev.key.node_name == key.node_name;
            if volume_name_match && pod_name_match && node_name_match {
                // nonExactMatch pending first
                if prev.operation_pending {
                    return Some(i);
                }
                // nonExactMatch with no pending, set opIndex to the first
                // nonExactMatch; exactMatch can override opIndex to expected
                if op_index.is_none() || (pod_name_exact_match && node_name_exact_match) {
                    op_index = Some(i);
                }
            }
        }
        op_index
    }

    /// `getOperation`.
    fn get_operation(&self, key: &OperationKey) -> Option<usize> {
        self.operations.iter().position(|op| op.key == *key)
    }

    /// `deleteOperation`: "delete index without preserving order".
    fn delete_operation(&mut self, key: &OperationKey) {
        if let Some(i) = self.get_operation(key) {
            self.operations.swap_remove(i);
        }
    }
}

#[cfg(test)]
mod tests;
