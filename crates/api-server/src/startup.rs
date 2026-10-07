//! The single startup path shared by `main.rs` and [`crate::run`].
//!
//! Upstream has exactly one place where post-start hooks are registered:
//! `cmd/kube-apiserver/app/server.go:148` `Run` -> `CreateServerChain` ->
//! `server.PrepareRun()` (`:167`), with the hooks added via
//! `AddPostStartHookOrDie` in `pkg/controlplane/apiserver/server.go:146-319`
//! and `pkg/controlplane/instance.go:360-370`. The binary and the all-in-one
//! `run()` used to each carry a copy of this block, so a hook added to one
//! shipped missing from the other (#2490). Add new hooks HERE only; the
//! `startup_hooks_single_path` test enforces it.

use crate::{bootstrap, legacy_token_tracking, registry, storage_readiness_hook};
use rusternetes_storage::{Storage, StorageBackend};
use std::sync::Arc;
use tracing::{info, warn};

/// Bootstrap objects and register every apiserver post-start hook/controller.
pub async fn register_post_start_hooks(
    storage: Arc<StorageBackend>,
    api_port: u16,
    service_ip: std::net::IpAddr,
    service_cidrs: Vec<String>,
    ca_cert_pem: Option<&str>,
) {
    if let Err(e) =
        bootstrap::bootstrap_kubernetes_service(storage.clone(), api_port, service_ip).await
    {
        warn!(
            "Failed to bootstrap kubernetes Service Endpoints: {}. Continuing anyway.",
            e
        );
    }
    // systemnamespaces controller (upstream pkg/controlplane/controller/
    // systemnamespaces): NamespaceLifecycle needs these to exist (#2533).
    if let Err(e) = bootstrap::bootstrap_system_namespaces(storage.as_ref()).await {
        warn!(
            "Failed to bootstrap system namespaces: {}. Continuing anyway.",
            e
        );
    }
    // `rbac/bootstrap-roles` PostStartHook (upstream pkg/registry/rbac/rest/
    // storage_rbac.go:131-179), same as main.rs: the SAME hook, awaited so a
    // fresh store is never served with an empty RBAC policy (#2490).
    let _ = bootstrap::spawn_rbac_bootstrap_roles_hook(storage.clone()).await;
    // scheduling/bootstrap-system-priority-classes PostStartHook (upstream
    // pkg/registry/scheduling/rest/storage_scheduling.go), same as main.rs.
    bootstrap::spawn_system_priority_classes_hook(storage.clone());
    // start-system-namespaces-controller (server.go:145).
    bootstrap::spawn_system_namespaces_controller(storage.clone());
    // storage-readiness PostStartHook (server.go:315-317), behind
    // WatchCacheInitializationPostStartHook (off by default).
    storage_readiness_hook::spawn_for_backend(storage.clone());
    // Keep the kubernetes endpoint tracking the live api-server IP across
    // container recreates / IP changes (upstream EndpointReconciler, #1188).
    bootstrap::spawn_endpoint_reconciler(storage.clone(), api_port, service_ip);

    // Aggregation layer: probe aggregated APIService backends and set their
    // Available condition (upstream kube-aggregator availability controller,
    // which lives in the apiserver — not KCM).
    bootstrap::spawn_apiservice_availability_controller(storage.clone());

    // CRD controllers' resync (upstream post-start hook, apiextensions-apiserver
    // pkg/apiserver/apiserver.go:244-252): retries a CRD left Terminating.
    registry::apiextensions::customresourcedefinition::spawn_resync(storage.clone());
    // crd-informer-synced (apiserver.go:263): not ready until the CRDs are readable.
    registry::apiextensions::customresourcedefinition::spawn_crd_informer_synced_hook(
        storage.clone(),
    );

    // start-legacy-token-tracking-controller (server.go:319-322).
    legacy_token_tracking::spawn_legacy_token_tracking_controller(storage.clone());

    // The `kubernetes` ServiceCIDR, owned by the apiserver-side
    // default-ServiceCIDR controller (upstream
    // `pkg/controlplane/controller/defaultservicecidr`). Reconciles rather than
    // create-once: dual-stack upgrade, flag-mismatch warning, and `Ready=True`
    // only when the persisted CIDRs match this api-server's configuration.
    bootstrap::start_default_servicecidr_controller(storage.clone(), service_cidrs).await;

    // kube-system/extension-apiserver-authentication, kept by the
    // apiserver-side ClusterAuthenticationTrust controller (upstream
    // `pkg/controlplane/controller/clusterauthenticationtrust`).
    if let Err(e) =
        bootstrap::bootstrap_extension_apiserver_authentication_rbac(storage.clone()).await
    {
        warn!(
            "Failed to bootstrap extension-apiserver-authentication RBAC: {e}. Continuing anyway."
        );
    }
    bootstrap::spawn_cluster_authentication_trust_controller(
        storage.clone(),
        bootstrap::cluster_authentication_info(ca_cert_pem),
    );

    // Create default StorageClass (like k3s/kind ship with a default)
    {
        let sc_key = rusternetes_storage::build_key("storageclasses", None, "standard");
        if storage.get::<serde_json::Value>(&sc_key).await.is_err() {
            let storage_class = serde_json::json!({
                "apiVersion": "storage.k8s.io/v1",
                "kind": "StorageClass",
                "metadata": {
                    "name": "standard",
                    "uid": uuid::Uuid::new_v4().to_string(),
                    "creationTimestamp": chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                    "annotations": {
                        "storageclass.kubernetes.io/is-default-class": "true"
                    }
                },
                "provisioner": "rusternetes.io/hostpath",
                "reclaimPolicy": "Delete",
                "volumeBindingMode": "WaitForFirstConsumer"
            });
            if let Err(e) = storage.create(&sc_key, &storage_class).await {
                warn!("Failed to create default StorageClass: {}", e);
            } else {
                info!("Created default StorageClass 'standard' with rusternetes.io/hostpath provisioner");
            }
        }
    }
}

#[cfg(test)]
mod startup_hooks_single_path {
    const MAIN_RS: &str = include_str!("main.rs");
    const LIB_RS: &str = include_str!("lib.rs");

    /// Every hook/bootstrap entry point `register_post_start_hooks` owns. A
    /// call to any of these in `main.rs` or `lib.rs` means a second startup
    /// path is growing back (#2613, #2490).
    const HOOK_CALLS: &[&str] = &[
        "bootstrap::bootstrap_kubernetes_service",
        "bootstrap::bootstrap_system_namespaces",
        "bootstrap::spawn_",
        "bootstrap::start_default_servicecidr_controller",
        "bootstrap::bootstrap_extension_apiserver_authentication_rbac",
        "spawn_resync(",
        "spawn_crd_informer_synced_hook(",
        "spawn_legacy_token_tracking_controller(",
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
