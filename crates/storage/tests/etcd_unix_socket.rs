//! `EtcdStorage::new` must accept `unix:` etcd endpoints (issue #1574), the
//! way upstream's `clientv3` does (`translateEndpoint`,
//! `vendor/go.etcd.io/etcd/client/v3/internal/endpoint/endpoint.go`) so
//! kine/k3s/k0s need no socat bridge. Real etcd listens on a unix socket in a
//! bind-mounted directory. Soft-skips without Docker (same contract as
//! `etcd_rpc_subset.rs`).

use rusternetes_storage::{etcd::EtcdStorage, Storage};
use serde_json::json;
use testcontainers::{
    core::{Mount, WaitFor},
    runners::AsyncRunner,
    GenericImage, ImageExt, TestcontainersError,
};

#[tokio::test]
async fn etcd_over_unix_socket() {
    let dir = std::env::temp_dir().join(format!("etcd-uds-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let result = GenericImage::new("quay.io/coreos/etcd", "v3.5.17")
        .with_wait_for(WaitFor::message_on_stderr("ready to serve client requests"))
        .with_mount(Mount::bind_mount(dir.to_str().unwrap(), "/sock"))
        .with_cmd([
            "/usr/local/bin/etcd",
            "--name=etcd-uds",
            "--data-dir=/tmp/etcd-data",
            "--listen-client-urls=unix:///sock/etcd.sock:2379",
            "--advertise-client-urls=http://127.0.0.1:2379",
        ])
        .start()
        .await;
    let _container = match result {
        Ok(c) => c,
        Err(TestcontainersError::Client(testcontainers::core::client::ClientError::Init(e))) => {
            if std::env::var("STORAGE_CONTRACT_REQUIRE_DOCKER").as_deref() == Ok("1") {
                panic!("Docker unavailable ({e}) but STORAGE_CONTRACT_REQUIRE_DOCKER=1");
            }
            eprintln!("skipping etcd unix-socket test: Docker unavailable ({e})");
            return;
        }
        Err(e) => panic!("failed to start etcd: {e}"),
    };

    let sock = dir.join("etcd.sock:2379");
    // etcd (root in the container, no shell in the image) creates the socket
    // 0755 root-owned, so connect() as this user is denied; open it up with a
    // throwaway root container sharing the mount.
    let chmod = std::process::Command::new("docker")
        .args(["run", "--rm", "-v"])
        .arg(format!("{}:/sock", dir.display()))
        .args(["alpine:3", "chmod", "666", "/sock/etcd.sock:2379"])
        .status()
        .expect("run docker chmod");
    assert!(chmod.success(), "chmod of the etcd socket failed");
    let storage = EtcdStorage::new(vec![format!("unix://{}", sock.display())])
        .await
        .expect("connect over unix socket");
    let key = "/registry/uds/probe";
    storage
        .create(key, &json!({"hello": "uds"}))
        .await
        .expect("create over uds");
    let got: serde_json::Value = storage.get(key).await.expect("get over uds");
    assert_eq!(got["hello"], "uds");
    drop(storage);
    drop(_container);
    let _ = std::fs::remove_dir_all(&dir);
}
