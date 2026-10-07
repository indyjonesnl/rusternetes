//! #2490: `lib.rs::run` (the all-in-one entry point) must seed the default
//! RBAC policy through the same `rbac/bootstrap-roles` post-start hook as
//! `main.rs` (upstream pkg/registry/rbac/rest/storage_rbac.go:131-179
//! `PostStartHook`). Otherwise an authz-enabled all-in-one cluster starts
//! with an empty policy and nobody (not even `system:masters`) is allowed.

use rusternetes_api_server::{run, ApiServerConfig};
use rusternetes_storage::{memory::MemoryStorage, Storage, StorageBackend};
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
async fn run_seeds_cluster_admin_rbac_before_serving() {
    let mem = Arc::new(MemoryStorage::new());
    let storage = Arc::new(StorageBackend::Memory(mem.clone()));
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let config = ApiServerConfig {
        bind_address: addr.to_string(),
        skip_auth: false,
        ..ApiServerConfig::default()
    };
    let server = tokio::spawn(run(storage, config));

    let mut seeded = false;
    for _ in 0..100 {
        let role = mem
            .get::<serde_json::Value>("/registry/clusterroles/cluster-admin")
            .await;
        let binding = mem
            .get::<serde_json::Value>("/registry/clusterrolebindings/cluster-admin")
            .await;
        if role.is_ok() && binding.is_ok() {
            seeded = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    server.abort();
    assert!(
        seeded,
        "run() must seed cluster-admin ClusterRole + binding (rbac/bootstrap-roles hook)"
    );
}
