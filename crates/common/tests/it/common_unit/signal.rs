//! Unit tests moved from `src/signal.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use std::future::Future;
    pub use std::sync::Arc;
    pub use tokio::sync::watch;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::signal::*;
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
