//! Process signal handling for the control-plane binaries.
//!
//! Ported from `staging/src/k8s.io/apiserver/pkg/server/signal.go`
//! (`SetupSignalContext`, release-1.35): registered for SIGTERM and SIGINT; the
//! returned context is cancelled on the first signal, and a second signal
//! terminates the process with exit code 1.

use std::future::Future;
use std::sync::Arc;
use tokio::sync::watch;

/// Rust analogue of the `context.Context` that `SetupSignalContext` returns:
/// cancelled once, observable from any number of clones.
#[derive(Clone, Debug)]
pub struct ShutdownContext {
    tx: Arc<watch::Sender<bool>>,
}

impl Default for ShutdownContext {
    fn default() -> Self {
        Self::new()
    }
}

impl ShutdownContext {
    pub fn new() -> Self {
        let (tx, _rx) = watch::channel(false);
        Self { tx: Arc::new(tx) }
    }

    pub fn cancel(&self) {
        self.tx.send_replace(true);
    }

    pub fn is_cancelled(&self) -> bool {
        *self.tx.borrow()
    }

    /// Resolves when the context is cancelled (`ctx.Done()`).
    pub async fn cancelled(&self) {
        let mut rx = self.tx.subscribe();
        let _ = rx.wait_for(|c| *c).await;
    }
}

/// `SetupSignalContext`: SIGINT + SIGTERM cancel the context; a second signal
/// exits the process with status 1.
pub fn setup_signal_context() -> ShutdownContext {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut term = signal(SignalKind::terminate()).expect("install SIGTERM handler");
        let mut int = signal(SignalKind::interrupt()).expect("install SIGINT handler");
        // One channel for both signals, like `shutdownHandler` (cap 2).
        let (tx, mut rx) = tokio::sync::mpsc::channel::<()>(2);
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = term.recv() => {}
                    _ = int.recv() => {}
                }
                if tx.send(()).await.is_err() {
                    break;
                }
            }
        });
        setup_signal_context_with(
            async move {
                rx.recv().await;
                rx
            },
            |mut rx| async move {
                rx.recv().await;
            },
            |code| std::process::exit(code),
        )
    }
    #[cfg(not(unix))]
    {
        setup_signal_context_with(
            async {
                let _ = tokio::signal::ctrl_c().await;
            },
            |_| async {
                let _ = tokio::signal::ctrl_c().await;
            },
            |code| std::process::exit(code),
        )
    }
}

/// Injectable seam behind [`setup_signal_context`]: `first` resolving cancels
/// the context; the future built by `second` resolving calls `exit(1)`.
pub fn setup_signal_context_with<A, F, S, SF, E>(first: F, second: S, exit: E) -> ShutdownContext
where
    F: Future<Output = A> + Send + 'static,
    A: Send + 'static,
    S: FnOnce(A) -> SF + Send + 'static,
    SF: Future<Output = ()> + Send + 'static,
    E: FnOnce(i32) + Send + 'static,
{
    let ctx = ShutdownContext::new();
    let handler_ctx = ctx.clone();
    tokio::spawn(async move {
        let a = first.await;
        handler_ctx.cancel();
        second(a).await;
        exit(1); // second signal. Exit directly.
    });
    ctx
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::sync::{mpsc, oneshot};

    #[tokio::test]
    async fn first_signal_cancels_context_second_exits_1() {
        let (first_tx, first_rx) = oneshot::channel::<()>();
        let (second_tx, second_rx) = oneshot::channel::<()>();
        let (exit_tx, mut exit_rx) = mpsc::unbounded_channel::<i32>();
        let ctx = setup_signal_context_with(
            async move {
                let _ = first_rx.await;
            },
            move |_| async move {
                let _ = second_rx.await;
            },
            move |code| {
                let _ = exit_tx.send(code);
            },
        );
        assert!(!ctx.is_cancelled());
        first_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(2), ctx.cancelled())
            .await
            .expect("first signal must cancel the context");
        assert!(exit_rx.try_recv().is_err(), "one signal must not exit");
        second_tx.send(()).unwrap();
        let code = tokio::time::timeout(Duration::from_secs(2), exit_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(code, 1);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn real_sigterm_cancels_context() {
        // Registering the handler replaces the default SIGTERM action, so
        // raising it at ourselves cancels the context instead of killing us.
        let ctx = setup_signal_context();
        tokio::time::sleep(Duration::from_millis(100)).await;
        let pid = std::process::id().to_string();
        std::process::Command::new("kill")
            .args(["-TERM", &pid])
            .status()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), ctx.cancelled())
            .await
            .expect("SIGTERM must cancel the context");
    }
}
