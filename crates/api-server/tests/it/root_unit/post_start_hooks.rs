//! Unit tests moved from `src/post_start_hooks.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use std::collections::BTreeMap;
    pub use std::future::Future;
    pub use std::sync::atomic::{AtomicBool, Ordering};
    pub use std::sync::{Arc, Mutex, OnceLock};
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::post_start_hooks::*;
    use tokio::sync::oneshot;

    #[tokio::test]
    async fn unfinished_hook_fails_its_check_then_passes() {
        let reg = PostStartHooks::new();
        let (tx, rx) = oneshot::channel::<()>();
        let h = spawn_hook_with(
            &reg,
            "x/y",
            async move {
                rx.await.unwrap();
                Ok::<(), String>(())
            },
            |_, _| panic!("must not be fatal"),
        );
        assert_eq!(reg.check("x/y"), Some(Err("not finished")));
        assert_eq!(reg.checks()[0].0, "poststarthook/x/y");
        tx.send(()).unwrap();
        h.await.unwrap();
        assert_eq!(reg.check("x/y"), Some(Ok(())));
        assert_eq!(reg.check("nope"), None);
    }

    #[tokio::test]
    async fn failed_hook_is_fatal_and_never_finishes() {
        let reg = PostStartHooks::new();
        let (tx, rx) = std::sync::mpsc::channel::<String>();
        spawn_hook_with(
            &reg,
            "x/y",
            async { Err::<(), String>("boom".into()) },
            move |n, e| tx.send(format!("{n}: {e}")).unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(rx.recv().unwrap(), "x/y: boom");
        assert_eq!(reg.check("x/y"), Some(Err("not finished")));
    }

    async fn finished(reg: &PostStartHooks, name: &str) -> bool {
        for _ in 0..300 {
            if reg.check(name) == Some(Ok(())) {
                return true;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        false
    }

    /// server/config_test.go expects `generic-apiserver-start-informers`,
    /// `start-apiserver-admission-initializer` and (APF off)
    /// `max-in-flight-filter` as `poststarthook/<name>` checks, plus the
    /// `informer-sync` readyz check.
    #[tokio::test]
    async fn apf_off_registers_all_three_hooks_and_informer_sync() {
        let reg = PostStartHooks::new();
        register_generic_server_hooks(&reg, false);
        for name in [
            "generic-apiserver-start-informers",
            "start-apiserver-admission-initializer",
            "max-in-flight-filter",
        ] {
            assert!(finished(&reg, name).await, "{name} not registered/finished");
        }
        let names: Vec<_> = reg.checks().into_iter().map(|(n, _)| n).collect();
        assert!(names.contains(&"poststarthook/max-in-flight-filter".to_string()));
        assert_eq!(
            reg.boot_checks(),
            vec![("informer-sync".to_string(), Ok(()))]
        );
    }

    /// config.go:937-958: with APF on the watermark hook is
    /// `priority-and-fairness-filter`, never `max-in-flight-filter`.
    #[tokio::test]
    async fn apf_on_omits_max_in_flight_filter() {
        let reg = PostStartHooks::new();
        register_generic_server_hooks(&reg, true);
        assert!(finished(&reg, "generic-apiserver-start-informers").await);
        assert!(finished(&reg, "start-apiserver-admission-initializer").await);
        assert_eq!(reg.check("max-in-flight-filter"), None);
    }
}
