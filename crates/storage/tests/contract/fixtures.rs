//! Backend fixtures for the shared storage contract suite.
//!
//! Upstream instantiates its storage tests once per implementation (etcd3 and
//! the cacher); kine compiles the same suite and points it at itself. We do the
//! same across MemoryStorage, etcd and kine.

use rusternetes_storage::{etcd::EtcdStorage, MemoryStorage};
use serde_json::{json, Value};
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    ContainerAsync, GenericImage, ImageExt, TestcontainersError,
};

/// A backend under test, plus the container keeping it alive (dropped = torn
/// down). `MemoryStorage` carries `None`.
pub struct Fixture<S> {
    pub storage: S,
    pub _container: Option<ContainerAsync<GenericImage>>,
}

/// Docker-less runners soft-skip instead of failing the job. Mirrors the helper
/// in `crates/storage/src/etcd.rs`.
fn is_docker_unavailable(err: &TestcontainersError) -> bool {
    matches!(
        err,
        TestcontainersError::Client(testcontainers::core::client::ClientError::Init(_))
    )
}

/// Escape hatch for environments where Docker is mandatory. Unset (the default,
/// i.e. a dev box) the container fixtures soft-skip; set to `1` they panic.
///
/// Without this, a runner with no Docker reports the whole suite green having
/// exercised only `MemoryStorage` — the backend with the most known gaps. CI
/// sets it on the DinD tier (`.github/workflows/storage-contract.yml`) so a
/// broken container setup fails loudly instead of silently shrinking coverage.
const REQUIRE_DOCKER_ENV: &str = "STORAGE_CONTRACT_REQUIRE_DOCKER";

/// Report a Docker-unavailable fixture: a soft skip by default, a panic under
/// `STORAGE_CONTRACT_REQUIRE_DOCKER=1`. `what` names the fixture in both.
fn skip_or_require(what: &str, err: &TestcontainersError) {
    if std::env::var(REQUIRE_DOCKER_ENV).as_deref() == Ok("1") {
        panic!("{what}: Docker unavailable ({err}), but {REQUIRE_DOCKER_ENV}=1 requires it");
    }
    eprintln!("skipping {what}: Docker unavailable ({err})");
}

/// An object shaped like the `example.Pod` upstream's suite stores: enough
/// metadata for `metadata.name` assertions and resourceVersion injection.
pub fn pod(namespace: &str, name: &str) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": name, "namespace": namespace },
        "spec": { "containers": [{ "name": "c", "image": "pause" }] }
    })
}

/// The storage key for a pod, matching `computePodKey` upstream:
/// `/registry/pods/<namespace>/<name>`.
pub fn pod_key(namespace: &str, name: &str) -> String {
    format!("/registry/pods/{namespace}/{name}")
}

pub fn memory() -> Option<Fixture<MemoryStorage>> {
    Some(Fixture {
        storage: MemoryStorage::new(),
        _container: None,
    })
}

async fn from_container(container: ContainerAsync<GenericImage>) -> Fixture<EtcdStorage> {
    let host = container.get_host().await.expect("container host");
    let port = container
        .get_host_port_ipv4(2379)
        .await
        .expect("mapped 2379");
    let storage = EtcdStorage::new(vec![format!("http://{host}:{port}")])
        .await
        .expect("connect to backend");
    Fixture {
        storage,
        _container: Some(container),
    }
}

pub async fn etcd() -> Option<Fixture<EtcdStorage>> {
    let started = GenericImage::new("quay.io/coreos/etcd", "v3.5.17")
        .with_exposed_port(2379.tcp())
        .with_wait_for(WaitFor::message_on_stderr("ready to serve client requests"))
        .with_cmd([
            "/usr/local/bin/etcd",
            "--name=etcd-contract",
            "--data-dir=/etcd-data",
            "--listen-client-urls=http://0.0.0.0:2379",
            "--advertise-client-urls=http://0.0.0.0:2379",
        ])
        .start()
        .await;
    match started {
        Ok(c) => Some(from_container(c).await),
        Err(e) if is_docker_unavailable(&e) => {
            skip_or_require("etcd contract suite", &e);
            None
        }
        Err(e) => panic!("failed to start etcd container: {e}"),
    }
}

pub async fn kine() -> Option<Fixture<EtcdStorage>> {
    // `/db` (not `/data/db`): the image ships that directory owned by `nobody`,
    // the uid kine runs as. See compose.kine.yml.
    let started = GenericImage::new("rancher/kine", "v0.13.11")
        .with_exposed_port(2379.tcp())
        .with_wait_for(WaitFor::message_on_stderr("Kine available at"))
        .with_cmd([
            "--endpoint=sqlite:///db/state.db",
            "--listen-address=0.0.0.0:2379",
        ])
        .start()
        .await;
    match started {
        Ok(c) => Some(from_container(c).await),
        Err(e) if is_docker_unavailable(&e) => {
            skip_or_require("kine contract suite", &e);
            None
        }
        Err(e) => panic!("failed to start kine container: {e}"),
    }
}
