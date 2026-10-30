//! Unit tests moved from `src/etcd.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use async_trait::async_trait;
    pub use etcd_client::{
        Client, Compare, CompareOp, GetOptions, PutOptions, TxnOp, WatchOptions,
    };
    pub use futures::StreamExt;
    pub use rusternetes_common::{authz::AuthzStorage, Error, Result};
    pub use rusternetes_storage::{Storage, WatchEvent, WatchStream};
    pub use serde::{de::DeserializeOwned, Serialize};
    pub use std::sync::atomic::{AtomicI64, Ordering};
    pub use std::sync::Arc;
    pub use tracing::{debug, error, info};
}

pub mod tests {
    /// rest.go:457-471, and the table in TestIsTooLargeError-style coverage:
    /// both upstream matches are too large, nothing else is.
    #[test]
    fn too_large_status_matches_upstream_is_too_large_error() {
        use tonic::{Code, Status};
        assert!(status_is_too_large(&Status::new(
            Code::InvalidArgument,
            "etcdserver: request is too large"
        )));
        assert!(status_is_too_large(&Status::new(
            Code::ResourceExhausted,
            "grpc: trying to send message larger than max (3145800 vs. 2097152)"
        )));
        // Right message, wrong code; right code, other message.
        assert!(!status_is_too_large(&Status::new(
            Code::Internal,
            "etcdserver: request is too large"
        )));
        assert!(!status_is_too_large(&Status::new(
            Code::InvalidArgument,
            "etcdserver: key is not provided"
        )));
        assert!(!status_is_too_large(&Status::new(
            Code::ResourceExhausted,
            "etcdserver: too many requests"
        )));
    }

    #[test]
    fn write_error_tags_only_too_large_grpc_statuses() {
        let big = etcd_client::Error::GRpcStatus(tonic::Status::new(
            tonic::Code::InvalidArgument,
            "etcdserver: request is too large",
        ));
        assert!(write_error("Failed to update resource", big).is_too_large_error());
        let other = etcd_client::Error::GRpcStatus(tonic::Status::new(
            tonic::Code::Unavailable,
            "etcdserver: leader changed",
        ));
        let e = write_error("Failed to update resource", other);
        assert!(matches!(e, Error::Storage(_)));
    }

    use super::__parent::*;
    use rusternetes_storage::etcd::*;
    use serde::{Deserialize, Serialize};

    #[test]
    fn test_inject_resource_version_with_existing() {
        let json = r#"{"metadata":{"name":"test","resourceVersion":"100"},"spec":{}}"#;
        let result = EtcdStorage::inject_resource_version(json, 200);
        assert!(result.contains("\"200\""));
        assert!(!result.contains("\"100\""));
    }

    #[test]
    fn test_inject_resource_version_without_existing() {
        let json = r#"{"metadata":{"name":"test"},"spec":{}}"#;
        let result = EtcdStorage::inject_resource_version(json, 42);
        assert!(result.contains("\"resourceVersion\":\"42\""));
    }

    #[test]
    fn test_inject_resource_version_empty_metadata() {
        let json = r#"{"metadata":{},"spec":{}}"#;
        let result = EtcdStorage::inject_resource_version(json, 99);
        assert!(result.contains("\"resourceVersion\":\"99\""));
    }

    // These tests exercise the real etcd backend by spinning up a disposable
    // etcd container via `testcontainers`. They require Docker (or a
    // Docker-compatible socket) on the host. The container is torn down
    // automatically when the `ContainerAsync` handle is dropped at the end of
    // the test, so no manual cleanup is required.
    //
    // On runners without a Docker socket (e.g. the ARC self-hosted runners
    // that back this repo's `cargo nextest` workflow), `start_etcd` returns
    // `None` and the test prints a skip message and exits 0 instead of
    // panicking. Don't promote this back to `expect("...")` — that's exactly
    // the regression that turned PR #746's nextest job red.
    //
    // The image and CLI flags match what `compose.yml` runs in production
    // (`quay.io/coreos/etcd:v3.5.17`, single-node, insecure client listener).

    use testcontainers::{
        core::{IntoContainerPort, WaitFor},
        runners::AsyncRunner,
        GenericImage, ImageExt, TestcontainersError,
    };

    /// Detects whether the host has a usable Docker (or Docker-compatible)
    /// socket. We treat any `Client(Init(...))` failure from testcontainers
    /// as "no Docker" — that variant wraps the bollard connect error, which
    /// fires both for a missing `/var/run/docker.sock` and for a refused
    /// connection to a custom `DOCKER_HOST`. Either way the test cannot run.
    fn is_docker_unavailable(err: &TestcontainersError) -> bool {
        matches!(
            err,
            TestcontainersError::Client(testcontainers::core::client::ClientError::Init(_))
        )
    }

    /// Boot a single-node etcd container and return an `EtcdStorage` pointing
    /// at it, alongside the container handle which must be kept alive for the
    /// duration of the test (drop = teardown).
    ///
    /// Returns `None` when Docker isn't reachable so callers can soft-skip
    /// rather than fail the whole nextest job on Docker-less runners.
    pub(super) async fn start_etcd(
    ) -> Option<(testcontainers::ContainerAsync<GenericImage>, EtcdStorage)> {
        // `quay.io/coreos/etcd` listens on 2379 for client gRPC and prints
        // "ready to serve client requests" once the listener is up. We bind
        // 0.0.0.0 so the port mapping works from the host network.
        let result = GenericImage::new("quay.io/coreos/etcd", "v3.5.17")
            .with_exposed_port(2379.tcp())
            .with_wait_for(WaitFor::message_on_stderr("ready to serve client requests"))
            .with_cmd([
                "/usr/local/bin/etcd",
                "--name=etcd-test",
                "--data-dir=/etcd-data",
                "--listen-client-urls=http://0.0.0.0:2379",
                "--advertise-client-urls=http://0.0.0.0:2379",
            ])
            .start()
            .await;

        let container = match result {
            Ok(c) => c,
            Err(e) if is_docker_unavailable(&e) => {
                eprintln!("skipping etcd integration test: Docker unavailable ({e})");
                return None;
            }
            Err(e) => panic!("failed to start etcd test container: {e}"),
        };

        let host = container
            .get_host()
            .await
            .expect("failed to resolve test container host");
        let port = container
            .get_host_port_ipv4(2379)
            .await
            .expect("failed to read mapped etcd client port");

        let endpoint = format!("http://{host}:{port}");
        let storage = EtcdStorage::new(vec![endpoint])
            .await
            .expect("failed to connect to test etcd");

        Some((container, storage))
    }

    #[tokio::test]
    async fn test_create_and_get() {
        let Some((_etcd, storage)) = start_etcd().await else {
            return;
        };

        #[derive(Debug, Serialize, Deserialize, PartialEq)]
        struct TestData {
            name: String,
            value: i32,
        }

        let data = TestData {
            name: "test".to_string(),
            value: 42,
        };

        let created = Storage::create(&storage, "/test/key", &data).await.unwrap();
        assert_eq!(created, data);

        let retrieved: TestData = Storage::get(&storage, "/test/key").await.unwrap();
        assert_eq!(retrieved, data);

        storage.delete("/test/key").await.unwrap();
    }

    /// A cached compaction floor must answer without a round trip, and must
    /// only ever answer for revisions it genuinely covers.
    ///
    /// This is the whole point of the cache. `is_revision_compacted` used to
    /// ask the backend every time, and the watch-cache reconnect path calls it
    /// on every reconnect. Measured on a live kine cluster: 43 rejected
    /// `Range /registry/` calls per second, sustained, kine pegged at ~1468%
    /// CPU (14.7 of 24 cores), and a 406-spec conformance phase that takes 27
    /// minutes on a healthy cluster still unfinished at 107.
    ///
    /// Upstream never asks: `CompactRevision()` reads an in-process integer
    /// (etcd3/compact.go:133-137) and it is part of the storage interface
    /// (storage/interfaces.go:278).
    #[test]
    fn cached_floor_answers_only_for_revisions_it_covers() {
        // Nothing observed yet: the floor cannot settle anything.
        assert_eq!(floor_settles_compaction(0, 1), None);
        assert_eq!(floor_settles_compaction(0, 9_999), None);

        // At or below a known floor is compacted for good — compaction is
        // monotonic, so this can be answered from memory forever.
        assert_eq!(floor_settles_compaction(100, 100), Some(true));
        assert_eq!(floor_settles_compaction(100, 99), Some(true));
        assert_eq!(floor_settles_compaction(100, 1), Some(true));

        // Above the floor the cache must NOT guess — that revision may still
        // be served, and answering `true` would 410 a healthy watch.
        assert_eq!(floor_settles_compaction(100, 101), None);
        assert_eq!(floor_settles_compaction(100, i64::MAX), None);
    }

    /// The floor must never move backwards.
    ///
    /// Mirrors upstream's `UpdateCompactRevision`, which is a max for the same
    /// reason (etcd3/compact.go:139-146):
    ///
    /// ```text
    /// c.compactRevision = max(c.compactRevision, rev)
    /// ```
    ///
    /// Concurrent probes finish out of order, so a late reply carrying an
    /// older revision must not lower a floor a newer one already raised —
    /// that would send us back to asking the backend for revisions we have
    /// already proven compacted.
    #[test]
    fn compaction_floor_is_monotonic() {
        use std::sync::atomic::{AtomicI64, Ordering};
        let floor = AtomicI64::new(0);

        floor.fetch_max(500, Ordering::Relaxed);
        assert_eq!(floor.load(Ordering::Relaxed), 500);

        // An out-of-order observation of an older revision changes nothing.
        floor.fetch_max(120, Ordering::Relaxed);
        assert_eq!(
            floor.load(Ordering::Relaxed),
            500,
            "a stale observation must not lower the floor"
        );

        floor.fetch_max(900, Ordering::Relaxed);
        assert_eq!(floor.load(Ordering::Relaxed), 900);
    }
}

mod preconditions_tests {
    use crate::stor_unit::etcd::tests::start_etcd;
    use std::sync::Arc;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn delete_with_preconditions_suite() {
        let Some((_etcd, storage)) = start_etcd().await else {
            return;
        };
        crate::stor_unit::lib::preconditions_suite::run_all(Arc::new(storage)).await;
    }
}
