//! Unit tests moved from `src/pluginmanager/operation.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use hyper_util::rt::TokioIo;
    pub use rusternetes_kubelet::pluginmanager::cache::{
        ActualStateOfWorld, PluginHandler, PluginInfo,
    };
    pub use rusternetes_kubelet::pluginmanager::operation::registerapi::registration_client::RegistrationClient;
    pub use rusternetes_kubelet::pluginmanager::operation::registerapi::{
        InfoRequest, RegistrationStatus,
    };
    pub use rusternetes_kubelet::volume_plugins::util::exponential_backoff::ExponentialBackoff;
    pub use std::collections::HashMap;
    pub use std::sync::{Arc, Mutex};
    pub use std::time::Duration;
    pub use tonic::transport::{Channel, Endpoint, Uri};
    pub use tower::service_fn;
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubelet::pluginmanager::operation::*;
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
