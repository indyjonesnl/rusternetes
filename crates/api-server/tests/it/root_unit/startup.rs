//! Unit tests moved from `src/startup.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_api_server::{
        apiserver_identity, bootstrap, legacy_token_tracking, registry, storage_readiness_hook,
    };
    pub use rusternetes_storage::{Storage, StorageBackend};
    pub use std::sync::Arc;
    pub use tracing::{info, warn};
}

mod aggregator_hooks {
    /// kube-aggregator `apiserver.go:368-420`: `apiservice-registration-controller`
    /// and `apiservice-discovery-controller` are registered post-start hooks, and
    /// both finish once the registration controller has installed every known
    /// APIService (`apiServiceRegistrationControllerInitiated`).
    #[tokio::test]
    async fn startup_registers_and_finishes_the_apiservice_hooks() {
        let storage = std::sync::Arc::new(rusternetes_storage::StorageBackend::new_memory());
        rusternetes_api_server::startup::register_post_start_hooks(
            storage,
            6443,
            "10.96.0.1".parse().unwrap(),
            rusternetes_api_server::bootstrap::AdvertiseAddress::new(None, "0.0.0.0:6443"),
            vec!["10.96.0.0/12".to_string()],
            rusternetes_api_server::startup::ClusterAuthnInputs::default(),
            |_| {},
        )
        .await;
        for name in [
            "apiservice-registration-controller",
            "apiservice-discovery-controller",
        ] {
            let mut finished = false;
            for _ in 0..300 {
                if rusternetes_api_server::post_start_hooks::global().check(name) == Some(Ok(())) {
                    finished = true;
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            assert!(finished, "{name} not registered/finished");
        }
    }
}

mod startup_hooks_single_path {
    const MAIN_RS: &str = include_str!("../../../src/main.rs");
    const LIB_RS: &str = include_str!("../../../src/lib.rs");

    /// Every hook/bootstrap entry point `register_post_start_hooks` owns. A
    /// call to any of these in `main.rs` or `lib.rs` means a second startup
    /// path is growing back (#2613, #2490).
    const HOOK_CALLS: &[&str] = &[
        "bootstrap::bootstrap_kubernetes_service",
        "bootstrap::bootstrap_system_namespaces",
        "bootstrap::spawn_",
        "bootstrap::start_default_servicecidr_controller",
        "bootstrap::bootstrap_extension_apiserver_authentication_rbac",
        // `start_controllers(` is the one exception: the entry points pass it
        // in as a closure (see `spawn_crd_resync`), each over its own state.
        "spawn_crd_informer_synced_hook(",
        "spawn_apiservice_registration_hooks(",
        "spawn_legacy_token_tracking_controller(",
        "spawn_identity_hooks(",
        "storage_readiness_hook::spawn_for_backend(",
    ];

    /// Source of an entry point without its test module.
    fn production(src: &str) -> &str {
        src.split("#[cfg(test)]").next().unwrap()
    }

    #[test]
    fn both_entry_points_call_the_shared_registration() {
        for (name, src) in [("main.rs", MAIN_RS), ("lib.rs", LIB_RS)] {
            assert_eq!(
                production(src)
                    .matches("startup::register_post_start_hooks(")
                    .count(),
                1,
                "{name} must call startup::register_post_start_hooks exactly once"
            );
        }
    }

    #[test]
    fn entry_points_register_no_hooks_of_their_own() {
        for (name, src) in [("main.rs", MAIN_RS), ("lib.rs", LIB_RS)] {
            for call in HOOK_CALLS {
                assert!(
                    !production(src).contains(call),
                    "{name} calls `{call}` directly; register it in \
                     startup::register_post_start_hooks so both entry points get it"
                );
            }
        }
    }
}
