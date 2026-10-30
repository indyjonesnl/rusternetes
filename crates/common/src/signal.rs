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
