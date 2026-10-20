//! A failed RestoreAll returns before Healthz.Updated in upstream
//! pkg/proxy/iptables/proxier.go:1497-1506,1521-1522. Retrying identical state
//! must attempt the dataplane again rather than reporting a cached success.
use super::*;
use rusternetes_storage::memory::MemoryStorage;
use std::os::unix::fs::PermissionsExt;

#[tokio::test]
async fn failed_sync_is_not_cached() {
    if std::env::var_os("RUSTERNETES_TEST_SYNC_FAILURE").is_none() {
        // Run in a child so PATH isolation cannot affect other tests. Both
        // binaries are failing scripts: this test never invokes host iptables.
        let directory =
            std::env::temp_dir().join(format!("rusternetes-3477-sync-{}", std::process::id()));
        std::fs::create_dir_all(&directory).unwrap();
        for binary in ["iptables", "iptables-restore"] {
            let path = directory.join(binary);
            std::fs::write(&path, "#!/bin/sh\nexit 1\n").unwrap();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "proxy::failure_tests::failed_sync_is_not_cached",
                "--nocapture",
            ])
            .env("RUSTERNETES_TEST_SYNC_FAILURE", "1")
            .env("PATH", &directory)
            .status()
            .unwrap();
        std::fs::remove_dir_all(directory).unwrap();
        assert!(status.success());
        return;
    }
    let storage = Arc::new(MemoryStorage::new());
    let service: Service = serde_json::from_value(serde_json::json!({
        "apiVersion": "v1", "kind": "Service", "metadata": {"name": "s", "namespace": "default"},
        "spec": {"clusterIP": "10.96.0.1", "ports": [{"port": 80}]}
    }))
    .unwrap();
    storage
        .create("/registry/services/default/s", &service)
        .await
        .unwrap();
    let mut proxy = KubeProxy {
        storage,
        iptables: IptablesManager::for_testing(false), // skips initialization/sysctls
        node_name: "n".into(),
        last_sync_hash: 0,
    };
    assert!(
        proxy.sync().await.is_err(),
        "first failed apply must fail sync"
    );
    assert!(
        proxy.sync().await.is_err(),
        "failed state must never become a cached successful sync"
    );
}
