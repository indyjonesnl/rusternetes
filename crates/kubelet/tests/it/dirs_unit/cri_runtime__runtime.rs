//! Unit tests moved from `src/cri_runtime/runtime.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use chrono::Utc;
    pub use rusternetes_common::resources::pod::{
        Container, ContainerState, ContainerStateRunning, ContainerStateTerminated,
        ContainerStateWaiting, ContainerStatus, Pod, Probe,
    };
    pub use rusternetes_common::resources::{ConfigMap, Secret, Service};
    pub use rusternetes_cri::{v1, CriClient, CriError};
    pub use rusternetes_kubelet::cri_runtime::{image_volume_metrics, probe, status, translate};
    pub use rusternetes_kubelet::server_metrics_endpoints::{
        observe_probe, ProbeResult, ProbeType,
    };
    pub use rusternetes_storage::{build_prefix, Storage};
    pub use std::path::Path;
    pub use std::sync::{Arc, Mutex};
    pub use tracing::{debug, warn};
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::pod::HTTPGetAction;
    use rusternetes_common::resources::pod::PodSpec;
    use rusternetes_common::resources::pod::PodStatus;
    use rusternetes_common::resources::policy::IntOrString;
    use rusternetes_common::resources::Event;
    use rusternetes_kubelet::cri_runtime::runtime::*;
    use rusternetes_storage::{Storage, StorageBackend};
    use std::collections::HashMap;

    /// Regression for the in-place-resize specs: the resize was silently
    /// dropped because the caller passed a `<pod>_<container>` composite name
    /// while the CRI label holds the BARE container name
    /// (`translate.rs:1208` inserts `container.name`), so the filter matched
    /// nothing and `update_container_resources` returned `Ok(())` while the
    /// kubelet logged "Updated container ... (resize)".
    ///
    /// The filter must also be pod-scoped: the bare name alone matches every
    /// pod on the node with a container called `c1`, and the resize specs run
    /// identically-named containers in concurrent namespaces.
    /// Regression: the resize path used to re-derive the cgroup values by hand
    /// (`kubelet.rs`, `let quota = (millicores * period) / 1000`) instead of
    /// reusing the create path's `translate::linux_resources`. It therefore
    /// skipped upstream's `MinQuotaPeriod` floor, and a sub-10m CPU limit
    /// produced `cpu.cfs_quota_us = 500` — below the kernel's 1000µs minimum.
    /// crun rejected the write:
    ///
    /// ```text
    /// crun did not terminate successfully: write to `cpu.max`: Invalid argument
    /// ```
    ///
    /// leaving the pod wedged in `resize: InProgress` forever. Both paths must
    /// produce identical values for the same container.
    #[test]
    fn resize_values_floor_tiny_cpu_like_the_create_path() {
        let container = Container {
            name: "c1".to_string(),
            resources: Some(rusternetes_common::types::ResourceRequirements {
                requests: Some(HashMap::from([("cpu".to_string(), "1m".to_string())])),
                limits: Some(HashMap::from([("cpu".to_string(), "5m".to_string())])),
                claims: None,
            }),
            ..Default::default()
        };

        let r = translate::linux_resources(&container).expect("resources");

        assert_eq!(
            r.cpu_quota, 1000,
            "5m -> 500µs must be floored to the kernel minimum 1000µs, not written raw"
        );
        assert_eq!(r.cpu_period, 100_000);
        assert_eq!(
            r.cpu_shares, 2,
            "1m -> 1 share must be raised to MinShares (2)"
        );
    }

    #[test]
    fn resize_filter_is_pod_scoped_and_uses_the_bare_container_name() {
        let filter = resize_container_filter("uid-123", "c1");
        let sel = filter.label_selector;

        assert_eq!(
            sel.get(translate::labels::CONTAINER_NAME)
                .map(String::as_str),
            Some("c1"),
            "must match the label CRI actually carries, not a <pod>_<container> composite"
        );
        assert_eq!(
            sel.get(translate::labels::POD_UID).map(String::as_str),
            Some("uid-123"),
            "an unscoped name matches other pods' containers of the same name"
        );
        assert_eq!(sel.len(), 2, "both labels, nothing else");
    }

    #[test]
    fn reserved_sandbox_id_parses_containerd_reserved_error() {
        // The exact shape containerd emits on a sandbox-name reservation
        // conflict (observed live in #1050).
        let msg = "failed to reserve sandbox name \
            \"netserver-0_pod-network-test-5426_8f5e84d1_0\": name \
            \"netserver-0_pod-network-test-5426_8f5e84d1_0\" is reserved for \
            \"637489deadebe17e8bc434d0bb3bffb2d7fdc1c83b42055b4cc20fddadc93c78\"";
        let err = CriError::Rpc {
            rpc: "RunPodSandbox",
            source: Box::new(tonic::Status::failed_precondition(msg)),
        };
        assert_eq!(
            reserved_sandbox_id(&err).as_deref(),
            Some("637489deadebe17e8bc434d0bb3bffb2d7fdc1c83b42055b4cc20fddadc93c78")
        );

        // An unrelated RPC error must not match (we'd wrongly remove a sandbox).
        let other = CriError::Rpc {
            rpc: "RunPodSandbox",
            source: Box::new(tonic::Status::unavailable("connection refused")),
        };
        assert_eq!(reserved_sandbox_id(&other), None);
    }
    use std::sync::Arc;

    /// Build a minimal `ContainerStatus` for the restart-attempt tests.
    fn test_container_status(
        name: &str,
        restart_count: u32,
        state: Option<ContainerState>,
        container_id: Option<String>,
    ) -> ContainerStatus {
        ContainerStatus {
            name: name.to_string(),
            ready: false,
            restart_count,
            state,
            last_state: None,
            image: None,
            image_id: None,
            container_id,
            started: None,
            allocated_resources: None,
            allocated_resources_status: None,
            resources: None,
            user: None,
            volume_mounts: None,
            stop_signal: None,
        }
    }

    fn pod_with_statuses(
        containers: Vec<ContainerStatus>,
        init_containers: Vec<ContainerStatus>,
    ) -> Pod {
        let mut pod = Pod::new("p", PodSpec::default());
        pod.metadata.uid = "uid-1".to_string();
        pod.status = Some(PodStatus {
            container_statuses: Some(containers),
            init_container_statuses: Some(init_containers),
            ..Default::default()
        });
        pod
    }

    #[test]
    fn next_restart_attempt_is_zero_on_first_start() {
        // No status at all → first start → attempt 0.
        let pod = Pod::new("p", PodSpec::default());
        assert_eq!(next_restart_attempt(&pod, "c"), 0);

        // A pre-start `Waiting`/`PodInitializing` placeholder (no container id,
        // never ran) must NOT be counted as a prior run — still attempt 0.
        let waiting = test_container_status(
            "c",
            0,
            Some(ContainerState {
                waiting: Some(ContainerStateWaiting {
                    reason: Some("PodInitializing".to_string()),
                    message: None,
                }),
                ..Default::default()
            }),
            None,
        );
        let pod = pod_with_statuses(vec![waiting], vec![]);
        assert_eq!(next_restart_attempt(&pod, "c"), 0);
    }

    #[test]
    fn next_restart_attempt_is_prior_count_plus_one_after_a_run() {
        // Prior instance actually ran (has a container id) with restartCount 0
        // → this (re)start is attempt 1. Reproduces the fix for the
        // liveness/startup-probe restart that used to reset 1 → 0: the count is
        // read from the persisted status, which survives sandbox teardown.
        let terminated = test_container_status(
            "c",
            0,
            Some(ContainerState {
                terminated: Some(ContainerStateTerminated {
                    exit_code: 137,
                    signal: None,
                    reason: None,
                    message: None,
                    started_at: None,
                    finished_at: None,
                    container_id: None,
                }),
                ..Default::default()
            }),
            Some("cid-0".to_string()),
        );
        let pod = pod_with_statuses(vec![terminated], vec![]);
        assert_eq!(next_restart_attempt(&pod, "c"), 1);

        // Monotonic: a prior status already at restartCount 2 → next is 3.
        let running = test_container_status(
            "c",
            2,
            Some(ContainerState {
                running: Some(ContainerStateRunning { started_at: None }),
                ..Default::default()
            }),
            Some("cid-2".to_string()),
        );
        let pod = pod_with_statuses(vec![running], vec![]);
        assert_eq!(next_restart_attempt(&pod, "c"), 3);
    }

    #[test]
    fn next_restart_attempt_reads_init_container_statuses() {
        // Restartable init containers (sidecars) carry their count in
        // init_container_statuses; a sidecar restart must be counted there too.
        let sidecar = test_container_status(
            "side",
            1,
            Some(ContainerState {
                running: Some(ContainerStateRunning { started_at: None }),
                ..Default::default()
            }),
            Some("cid".to_string()),
        );
        let pod = pod_with_statuses(vec![], vec![sidecar]);
        assert_eq!(next_restart_attempt(&pod, "side"), 2);
    }

    #[test]
    fn plain_init_container_ready_only_after_successful_exit() {
        let term = |code| {
            Some(ContainerState {
                terminated: Some(ContainerStateTerminated {
                    exit_code: code,
                    signal: None,
                    reason: None,
                    message: None,
                    started_at: None,
                    finished_at: None,
                    container_id: None,
                }),
                ..Default::default()
            })
        };
        let running = Some(ContainerState {
            running: Some(ContainerStateRunning { started_at: None }),
            ..Default::default()
        });
        // Plain init container: Ready only once it has terminated with exit 0
        // (upstream prober_manager). A non-zero exit or still-running = not ready.
        assert!(init_container_ready(&term(0), false, false));
        assert!(!init_container_ready(&term(1), false, false));
        assert!(!init_container_ready(&running, false, false));
        // Restartable init (sidecar) keeps its prior running/probe-based readiness.
        assert!(init_container_ready(&running, true, true));
        assert!(!init_container_ready(&running, true, false));
    }

    #[test]
    fn grpc_probe_only_serving_status_is_healthy() {
        use tonic_health::pb::health_check_response::ServingStatus;
        // The kubelet grpc prober treats only SERVING as healthy; every other
        // status (and an unreachable server) is a probe failure.
        assert!(grpc_status_healthy(ServingStatus::Serving as i32));
        assert!(!grpc_status_healthy(ServingStatus::NotServing as i32));
        assert!(!grpc_status_healthy(ServingStatus::Unknown as i32));
        assert!(!grpc_status_healthy(ServingStatus::ServiceUnknown as i32));
    }

    #[test]
    fn exec_probe_passes_only_on_zero_exit_within_timeout() {
        // Healthy iff the command completed with exit 0 within timeoutSeconds.
        // A timed-out exec (None) fails — the command outran its timeout, which
        // the kubelet exec prober treats as a probe failure (→ restart).
        assert!(exec_probe_passed(Some(0)));
        assert!(!exec_probe_passed(Some(1)));
        assert!(!exec_probe_passed(None));
    }

    #[test]
    fn readiness_state_machine_latches_on_thresholds() {
        let mut st = ProbeState::default();
        // Not ready until the first success (successThreshold 1).
        assert!(!st.ready);
        assert!(readiness_after_observation(&mut st, true, 1, 3));
        // Single/second failure with failureThreshold 3 holds ready=true.
        assert!(readiness_after_observation(&mut st, false, 1, 3));
        assert!(readiness_after_observation(&mut st, false, 1, 3));
        // Third consecutive failure crosses the threshold → not ready.
        assert!(!readiness_after_observation(&mut st, false, 1, 3));
        // A later success (threshold 1) recovers readiness.
        assert!(readiness_after_observation(&mut st, true, 1, 3));
    }

    #[tokio::test]
    async fn apply_readiness_updates_restartable_init_sidecars_only() {
        let runtime = CriContainerRuntime::connect(
            "/tmp/rusternetes-test-missing-cri.sock",
            "/tmp/rusternetes-test-logs",
        )
        .await
        .expect("runtime handle is lazy and should not dial during construction");
        let plain_init = Container {
            name: "plain-init".to_string(),
            image: "busybox".to_string(),
            readiness_probe: Some(serde_json::from_str::<Probe>("{}").unwrap()),
            ..Default::default()
        };
        let sidecar = Container {
            name: "sidecar".to_string(),
            image: "busybox".to_string(),
            restart_policy: Some("Always".to_string()),
            readiness_probe: Some(serde_json::from_str::<Probe>("{}").unwrap()),
            ..Default::default()
        };
        let app = Container {
            name: "app".to_string(),
            image: "busybox".to_string(),
            ..Default::default()
        };
        let pod = Pod::new(
            "sidecar-ready",
            PodSpec {
                containers: vec![app],
                init_containers: Some(vec![plain_init, sidecar]),
                ..Default::default()
            },
        );
        let running = Some(ContainerState {
            running: Some(ContainerStateRunning { started_at: None }),
            ..Default::default()
        });
        let mut statuses = vec![
            ContainerStatus {
                name: "plain-init".to_string(),
                ready: false,
                state: running.clone(),
                ..waiting_status("plain-init")
            },
            ContainerStatus {
                name: "sidecar".to_string(),
                ready: false,
                state: running,
                ..waiting_status("sidecar")
            },
        ];

        runtime.apply_readiness(&pod, &mut statuses).await;

        assert!(
            !statuses[0].ready,
            "plain init containers are not readiness-probe targets"
        );
        assert!(
            statuses[1].ready,
            "restartable init sidecars must get readiness-probe results"
        );
    }

    async fn spawn_http_probe_redirect_server() -> u16 {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                tokio::spawn(async move {
                    let mut buf = [0_u8; 1024];
                    let n = socket.read(&mut buf).await.unwrap_or(0);
                    let request = String::from_utf8_lossy(&buf[..n]);
                    let path = request
                        .lines()
                        .next()
                        .and_then(|line| line.split_whitespace().nth(1))
                        .unwrap_or("/");
                    let response = match path {
                        "/redirect-local" => {
                            "HTTP/1.1 302 Found\r\nLocation: /fail\r\nContent-Length: 0\r\n\r\n"
                        }
                        "/redirect-nonlocal" => {
                            concat!(
                                "HTTP/1.1 302 Found\r\n",
                                "Location: http://0.0.0.0/fail\r\n",
                                "Content-Length: 40\r\n",
                                "\r\n",
                                "<a href=\"http://0.0.0.0/fail\">Found</a>."
                            )
                        }
                        "/fail" => {
                            "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n"
                        }
                        _ => "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok",
                    };
                    let _ = socket.write_all(response.as_bytes()).await;
                });
            }
        });
        port
    }

    async fn spawn_lifecycle_hook_http_server(response: &'static str) -> u16 {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                tokio::spawn(async move {
                    let mut buf = [0_u8; 1024];
                    let _ = socket.read(&mut buf).await;
                    let _ = socket.write_all(response.as_bytes()).await;
                });
            }
        });
        port
    }

    fn http_probe(path: &str, port: u16) -> Probe {
        Probe {
            http_get: Some(HTTPGetAction {
                path: Some(path.to_string()),
                port: IntOrString::Int(i32::from(port)),
                host: Some("127.0.0.1".to_string()),
                scheme: Some("HTTP".to_string()),
                http_headers: None,
            }),
            ..serde_json::from_str::<Probe>("{}").unwrap()
        }
    }

    fn probe_test_container() -> Container {
        Container {
            name: "app".to_string(),
            image: "busybox".to_string(),
            ..Default::default()
        }
    }

    fn lifecycle_test_pod_and_container() -> (Pod, Container) {
        let container = Container {
            name: "app".to_string(),
            image: "busybox".to_string(),
            ..Default::default()
        };
        let pod = Pod::new(
            "lifecycle-hook-test",
            PodSpec {
                containers: vec![container.clone()],
                ..Default::default()
            },
        );
        (pod, container)
    }

    #[tokio::test]
    async fn lifecycle_http_hook_ignores_non_2xx_status() {
        let port =
            spawn_lifecycle_hook_http_server("HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n")
                .await;
        let runtime = CriContainerRuntime::connect(
            "/tmp/rusternetes-test-missing-cri.sock",
            "/tmp/rusternetes-test-logs",
        )
        .await
        .unwrap();
        let (pod, container) = lifecycle_test_pod_and_container();
        let handler = rusternetes_common::resources::pod::LifecycleHandler {
            exec: None,
            http_get: Some(HTTPGetAction {
                path: Some("/echo?msg=poststart".to_string()),
                port: IntOrString::Int(i32::from(port)),
                host: Some("127.0.0.1".to_string()),
                scheme: Some("HTTP".to_string()),
                http_headers: None,
            }),
            tcp_socket: None,
            sleep: None,
        };

        runtime
            .run_lifecycle_handler(&pod, &container, "unused", &handler)
            .await
            .expect("lifecycle hooks should ignore HTTP status codes");
    }

    #[tokio::test]
    async fn lifecycle_https_hook_falls_back_to_http() {
        let port =
            spawn_lifecycle_hook_http_server("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
                .await;
        let runtime = CriContainerRuntime::connect(
            "/tmp/rusternetes-test-missing-cri.sock",
            "/tmp/rusternetes-test-logs",
        )
        .await
        .unwrap();
        let (pod, container) = lifecycle_test_pod_and_container();
        let handler = rusternetes_common::resources::pod::LifecycleHandler {
            exec: None,
            http_get: Some(HTTPGetAction {
                path: Some("/echo?msg=prestop".to_string()),
                port: IntOrString::Int(i32::from(port)),
                host: Some("127.0.0.1".to_string()),
                scheme: Some("HTTPS".to_string()),
                http_headers: None,
            }),
            tcp_socket: None,
            sleep: None,
        };

        runtime
            .run_lifecycle_handler(&pod, &container, "unused", &handler)
            .await
            .expect("HTTPS lifecycle hooks must fall back to HTTP");
    }

    /// Spawn a genuine TLS server (self-signed cert) that captures the first
    /// request line it receives and replies `200 OK`. Returns `(port, rx)`
    /// where `rx` yields the captured request line once a request arrives.
    ///
    /// This mirrors the NodeConformance `prestop https hook` server: agnhost
    /// `netexec --tls-cert-file …` speaks real TLS on :9090, so the lifecycle
    /// handler's httpGet must complete a TLS handshake — not fall back to
    /// plaintext HTTP (which a TLS-only server never answers).
    async fn spawn_lifecycle_hook_https_server() -> (u16, tokio::sync::oneshot::Receiver<String>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let cert_der = certified.cert.der().clone();
        let key_der =
            rustls::pki_types::PrivatePkcs8KeyDer::from(certified.key_pair.serialize_der());
        // Install a process-default crypto provider for the server side; the
        // reqwest client bundles its own. Ignore the error if another test
        // already installed one.
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert_der], key_der.into())
            .unwrap();
        let acceptor = tokio_rustls::TlsAcceptor::from(std::sync::Arc::new(config));

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (tx, rx) = tokio::sync::oneshot::channel::<String>();
        tokio::spawn(async move {
            let mut tx = Some(tx);
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                let acceptor = acceptor.clone();
                let tx = tx.take();
                tokio::spawn(async move {
                    let Ok(mut tls) = acceptor.accept(stream).await else {
                        return;
                    };
                    let mut buf = [0_u8; 1024];
                    let n = tls.read(&mut buf).await.unwrap_or(0);
                    let req = String::from_utf8_lossy(&buf[..n]);
                    let first_line = req.lines().next().unwrap_or("").to_string();
                    let _ = tls
                        .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
                        .await;
                    let _ = tls.shutdown().await;
                    if let Some(tx) = tx {
                        let _ = tx.send(first_line);
                    }
                });
            }
        });
        (port, rx)
    }

    /// Regression for the NodeConformance `[FeatureGate:SidecarContainers]
    /// Restartable Init Container Lifecycle Hook … should execute prestop https
    /// hook properly` failure: the preStop httpGet targets a *real* TLS server
    /// (scheme `HTTPS`), so the handler must complete the TLS handshake and
    /// deliver the request. The pre-existing fallback test only covered a
    /// plaintext server, hiding this path.
    #[tokio::test]
    async fn lifecycle_https_hook_reaches_real_tls_server() {
        let (port, rx) = spawn_lifecycle_hook_https_server().await;
        let runtime = CriContainerRuntime::connect(
            "/tmp/rusternetes-test-missing-cri.sock",
            "/tmp/rusternetes-test-logs",
        )
        .await
        .unwrap();
        let (pod, container) = lifecycle_test_pod_and_container();
        let handler = rusternetes_common::resources::pod::LifecycleHandler {
            exec: None,
            http_get: Some(HTTPGetAction {
                path: Some("/echo?msg=prestop".to_string()),
                port: IntOrString::Int(i32::from(port)),
                host: Some("127.0.0.1".to_string()),
                scheme: Some("HTTPS".to_string()),
                http_headers: None,
            }),
            tcp_socket: None,
            sleep: None,
        };

        runtime
            .run_lifecycle_handler(&pod, &container, "unused", &handler)
            .await
            .expect("HTTPS lifecycle hook must complete a real TLS handshake");

        let got = tokio::time::timeout(std::time::Duration::from_secs(5), rx)
            .await
            .expect("TLS server should have received the hook request")
            .expect("request line channel");
        assert!(
            got.contains("GET /echo?msg=prestop"),
            "TLS server received unexpected request line: {got:?}"
        );
    }

    #[tokio::test]
    async fn http_probe_follows_local_redirect_to_final_failure() {
        let port = spawn_http_probe_redirect_server().await;
        let runtime = CriContainerRuntime::connect(
            "/tmp/rusternetes-test-missing-cri.sock",
            "/tmp/rusternetes-test-logs",
        )
        .await
        .unwrap();
        let container = probe_test_container();
        let pod = Pod::new(
            "http-local-redirect",
            PodSpec {
                containers: vec![container.clone()],
                ..Default::default()
            },
        );

        let healthy = runtime
            .probe_container(&pod, &container, &http_probe("/redirect-local", port))
            .await
            .unwrap();

        assert!(
            !healthy,
            "same-host redirects must be followed and use the final response"
        );
    }

    #[tokio::test]
    async fn http_probe_non_local_redirect_is_warning_success() {
        let port = spawn_http_probe_redirect_server().await;
        let storage = Arc::new(StorageBackend::new_memory());
        let runtime = CriContainerRuntime::connect(
            "/tmp/rusternetes-test-missing-cri.sock",
            "/tmp/rusternetes-test-logs",
        )
        .await
        .unwrap()
        .with_event_recorder(Arc::clone(&storage));
        let container = probe_test_container();
        let mut pod = Pod::new(
            "http-nonlocal-redirect",
            PodSpec {
                containers: vec![container.clone()],
                ..Default::default()
            },
        );
        pod.metadata.namespace = Some("default".to_string());
        pod.metadata.uid = "pod-uid".to_string();

        let healthy = runtime
            .probe_container(&pod, &container, &http_probe("/redirect-nonlocal", port))
            .await
            .unwrap();

        assert!(
            healthy,
            "different-host redirects terminate with warning but count as success"
        );
        let obj = rusternetes_kubelet::events::container_object_reference(&pod, &container.name);
        let key = format!(
            "/registry/events/default/{}",
            Event::generate_name(&obj, rusternetes_kubelet::events::CONTAINER_PROBE_WARNING)
        );
        let event: Event = storage.get(&key).await.expect("warning event recorded");
        assert_eq!(
            event.reason,
            rusternetes_kubelet::events::CONTAINER_PROBE_WARNING
        );
        assert!(event
            .message
            .contains("Probe terminated redirects, Response body:"));
    }
    #[test]
    fn runtime_version_string_uses_cri_name_and_version() {
        // The k8s `containerRuntimeVersion` is `<runtime_name>://<runtime_version>`,
        // sourced from the CRI Version RPC — never a hardcoded literal.
        let v = v1::VersionResponse {
            version: "0.1.0".to_string(),
            runtime_name: "containerd-rs".to_string(),
            runtime_version: "0.1.2".to_string(),
            runtime_api_version: "v1".to_string(),
        };
        assert_eq!(format_runtime_version(&v), "containerd-rs://0.1.2");
    }

    #[test]
    fn find_container_searches_regular_and_init() {
        let spec = PodSpec {
            containers: vec![Container {
                name: "app".to_string(),
                ..Default::default()
            }],
            init_containers: Some(vec![Container {
                name: "setup".to_string(),
                termination_message_policy: Some("FallbackToLogsOnError".to_string()),
                ..Default::default()
            }]),
            ..Default::default()
        };
        let pod = Pod::new("web", spec);
        assert_eq!(
            find_container(&pod, "app").map(|c| c.name.as_str()),
            Some("app")
        );
        assert_eq!(
            find_container(&pod, "setup").and_then(|c| c.termination_message_policy.as_deref()),
            Some("FallbackToLogsOnError")
        );
        assert!(find_container(&pod, "missing").is_none());
    }

    #[test]
    fn restartable_init_container_is_detected_by_per_container_restart_policy() {
        let regular = Container {
            name: "init".to_string(),
            image: "busybox".to_string(),
            ..Default::default()
        };
        let sidecar = Container {
            name: "sidecar".to_string(),
            image: "busybox".to_string(),
            restart_policy: Some("Always".to_string()),
            ..Default::default()
        };

        assert!(!is_restartable_init_container(&regular));
        assert!(is_restartable_init_container(&sidecar));
    }

    #[test]
    fn read_log_tail_parses_cri_framing_and_limits_lines() {
        // Upstream readLastStringFromContainerLogs strips CRI log framing before
        // returning the tail (pkg/kubelet/kuberuntime/kuberuntime_container.go:591).
        // read_log_tail must do the same via rusternetes_cri::stream::read_log_file.
        let dir = tempfile::tempdir().unwrap();

        // CRI-formatted log: only the message part after stripping framing should appear.
        let cri_path = dir.path().join("cri.log");
        let cri_log = "2024-01-01T00:00:00.000000000Z stdout F DONE\n";
        std::fs::write(&cri_path, cri_log).unwrap();
        let tail = read_log_tail(cri_path.to_str().unwrap()).unwrap();
        assert_eq!(tail, "DONE\n", "CRI framing must be stripped");

        // Tail-lines cap: more than MAX_TERMINATION_MESSAGE_LOG_LINES lines → only
        // the last MAX lines are returned. Build 100 CRI-formatted lines, then ask
        // for the last 80 (= MAX_TERMINATION_MESSAGE_LOG_LINES).
        let many_path = dir.path().join("many.log");
        let mut many_log = String::new();
        for i in 0..100u32 {
            many_log.push_str(&format!(
                "2024-01-01T00:00:00.000000000Z stdout F line{}\n",
                i
            ));
        }
        std::fs::write(&many_path, &many_log).unwrap();
        let tail = read_log_tail(many_path.to_str().unwrap()).unwrap();
        // last 80 lines: line20..line99
        assert!(
            tail.starts_with("line20\n"),
            "tail must start at line20, got: {}",
            &tail[..20.min(tail.len())]
        );
        assert!(tail.ends_with("line99\n"));

        // Missing / empty logs yield None.
        assert!(read_log_tail(dir.path().join("nope.log").to_str().unwrap()).is_none());
        let empty = dir.path().join("empty.log");
        std::fs::write(&empty, "").unwrap();
        assert!(read_log_tail(empty.to_str().unwrap()).is_none());
    }

    #[test]
    fn service_env_derives_port_from_actual_service_port() {
        let map: HashMap<String, String> =
            service_env_vars("10.96.0.1", "8080").into_iter().collect();
        let get = |k: &str| map.get(k).map(String::as_str);

        // Link-var names embed the actual port, never a hardcoded 443.
        assert!(!map.keys().any(|k| k.contains("443")));
        assert_eq!(
            get("KUBERNETES_PORT_8080_TCP"),
            Some("tcp://10.96.0.1:8080")
        );
        assert_eq!(get("KUBERNETES_PORT_8080_TCP_PROTO"), Some("tcp"));
        assert_eq!(get("KUBERNETES_PORT_8080_TCP_PORT"), Some("8080"));
        assert_eq!(get("KUBERNETES_PORT_8080_TCP_ADDR"), Some("10.96.0.1"));
        assert_eq!(get("KUBERNETES_PORT"), Some("tcp://10.96.0.1:8080"));

        // Core + named-port alias (the Service port is named "https").
        assert_eq!(get("KUBERNETES_SERVICE_HOST"), Some("10.96.0.1"));
        assert_eq!(get("KUBERNETES_SERVICE_PORT"), Some("8080"));
        assert_eq!(get("KUBERNETES_SERVICE_PORT_HTTPS"), Some("8080"));
    }

    /// Regression test for NC failures:
    ///   "[sig-node] Sysctls … should support sysctls [Environment:NotInUserNS]"
    ///   "[sig-node] Sysctls … should support sysctls with slashes as separator"
    ///
    /// Both tests use `WaitForErrorEventOrSuccess` (test/e2e/framework/pod/pod_client.go:328)
    /// to detect pod start. That function watches pod Events and returns on the
    /// first event whose reason is one of:
    ///   - `"Started"` (container started successfully)
    ///   - `"Failed"`  (container start failed)
    ///   - `"Killing"` (container is being killed)
    ///   - `"SysctlForbidden"` (sysctl admission rejected the pod)
    ///
    /// Without a `Started` event the function spins until the context times out
    /// (→ `context deadline exceeded` at sysctl.go:99 / sysctl.go:209) even when
    /// the container is running fine. This test verifies that the `ContainerRuntime`
    /// event-emission machinery surfaces `Started` to a wired-up event recorder,
    /// which is the prerequisite for `create_and_start_container` to emit it.
    ///
    /// Upstream: `kuberuntime_container.go:298`
    ///   m.recordContainerEvent(ctx, pod, container, containerID,
    ///       v1.EventTypeNormal, events.StartedContainer, "Container started")
    #[tokio::test]
    async fn started_event_is_emitted_via_emit_event_and_visible_to_conformance_watcher() {
        use rusternetes_common::resources::pod::PodSpec;

        let storage = Arc::new(StorageBackend::new_memory());
        let runtime = CriContainerRuntime::connect(
            "/tmp/rusternetes-test-missing-cri.sock",
            "/tmp/rusternetes-test-logs",
        )
        .await
        .unwrap()
        .with_event_recorder(Arc::clone(&storage));

        let mut pod = Pod::new(
            "sysctl-test",
            PodSpec {
                containers: vec![rusternetes_common::resources::pod::Container {
                    name: "app".to_string(),
                    image: "busybox".to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            },
        );
        pod.metadata.namespace = Some("default".to_string());
        pod.metadata.uid = "sysctl-test-uid".to_string();

        // Simulate what create_and_start_container now does after start_container
        // succeeds: emit the "Started" event so WaitForErrorEventOrSuccess can
        // return instead of timing out.
        runtime
            .emit_event(
                &pod,
                Some("app"),
                rusternetes_kubelet::events::STARTED_CONTAINER,
                rusternetes_common::resources::EventType::Normal,
                "Started container app",
            )
            .await;

        // Verify the event is visible to a WaitForErrorEventOrSuccess-style watcher:
        // search for a "Started" reason event on this pod.
        let obj = rusternetes_kubelet::events::container_object_reference(&pod, "app");
        let key = format!(
            "/registry/events/default/{}",
            Event::generate_name(&obj, rusternetes_kubelet::events::STARTED_CONTAINER)
        );
        let ev: Event = storage.get(&key).await.expect(
            "Started event must be present so WaitForErrorEventOrSuccess can return; \
             without it NC sysctl tests time out with `context deadline exceeded`",
        );
        assert_eq!(
            ev.reason,
            rusternetes_kubelet::events::STARTED_CONTAINER,
            "reason must be \"Started\""
        );
        assert_eq!(
            ev.event_type,
            rusternetes_common::resources::EventType::Normal
        );
        // Container-scoped: fieldPath must identify the specific container.
        assert_eq!(
            ev.involved_object.field_path.as_deref(),
            Some("spec.containers{app}")
        );
    }

    /// Companion test for the Created event emitted before StartContainer.
    /// Upstream `kuberuntime_container.go:290`:
    ///   m.recordContainerEvent(…, events.CreatedContainer, "Container created")
    #[tokio::test]
    async fn created_event_is_emitted_via_emit_event() {
        use rusternetes_common::resources::pod::PodSpec;

        let storage = Arc::new(StorageBackend::new_memory());
        let runtime = CriContainerRuntime::connect(
            "/tmp/rusternetes-test-missing-cri.sock",
            "/tmp/rusternetes-test-logs",
        )
        .await
        .unwrap()
        .with_event_recorder(Arc::clone(&storage));

        let mut pod = Pod::new(
            "sysctl-created",
            PodSpec {
                containers: vec![rusternetes_common::resources::pod::Container {
                    name: "app".to_string(),
                    image: "busybox".to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            },
        );
        pod.metadata.namespace = Some("default".to_string());
        pod.metadata.uid = "sysctl-created-uid".to_string();

        runtime
            .emit_event(
                &pod,
                Some("app"),
                rusternetes_kubelet::events::CREATED_CONTAINER,
                rusternetes_common::resources::EventType::Normal,
                "Created container app",
            )
            .await;

        let obj = rusternetes_kubelet::events::container_object_reference(&pod, "app");
        let key = format!(
            "/registry/events/default/{}",
            Event::generate_name(&obj, rusternetes_kubelet::events::CREATED_CONTAINER)
        );
        let ev: Event = storage
            .get(&key)
            .await
            .expect("Created event must be present");
        assert_eq!(ev.reason, rusternetes_kubelet::events::CREATED_CONTAINER);
        assert_eq!(
            ev.event_type,
            rusternetes_common::resources::EventType::Normal
        );
    }

    /// #1554: a failed postStart lifecycle hook must kill the container and
    /// record a `FailedPostStartHook` **Warning** event, then fail the container
    /// start — mirroring upstream `startContainer`
    /// (`kuberuntime_container.go:319-335`), which records the event, calls
    /// `killContainer`, and returns `ErrPostStartHook`.
    ///
    /// This drives `fail_post_start_hook` directly (the full
    /// `create_and_start_container` path needs a live CRI): it asserts the
    /// Warning event lands with the generic, secret-safe message and that the
    /// returned error is the `PostStartHookError`, which the callers propagate
    /// via `?` to fail the start.
    #[tokio::test]
    async fn sandbox_handler_resolves_pod_runtime_class() {
        use rusternetes_common::resources::RuntimeClass;
        let storage = Arc::new(StorageBackend::new_memory());
        storage
            .create(
                &rusternetes_storage::build_key("runtimeclasses", None, "gvisor"),
                &RuntimeClass::new("gvisor", "runsc"),
            )
            .await
            .unwrap();
        let vm = rusternetes_kubelet::volumes::VolumeManager::new(
            "/tmp/rusternetes-test-vols".to_string(),
            Some(storage),
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
        );
        let rt = CriContainerRuntime::connect(
            "/tmp/rusternetes-test-missing-cri.sock",
            "/tmp/rusternetes-test-logs",
        )
        .await
        .unwrap()
        .with_volumes(vm);
        let mk = |rc: Option<&str>| {
            Pod::new(
                "p",
                PodSpec {
                    runtime_class_name: rc.map(str::to_string),
                    ..Default::default()
                },
            )
        };
        // `kuberuntime_sandbox.go:58` LookupRuntimeHandler(pod.Spec.RuntimeClassName)
        assert_eq!(
            rt.sandbox_runtime_handler(&mk(Some("gvisor")))
                .await
                .unwrap(),
            "runsc"
        );
        // No class resolves to "" (`runtimeclass_manager.go:61-65`: "The default
        // RuntimeClass always resolves to the empty runtime handler"); no
        // node-level CONTAINER_RUNTIME_HANDLER fallback (#3263).
        assert_eq!(rt.sandbox_runtime_handler(&mk(None)).await.unwrap(), "");
        assert_eq!(rt.sandbox_runtime_handler(&mk(Some(""))).await.unwrap(), "");
        // Missing RuntimeClass fails the sandbox (`kuberuntime_sandbox.go:60-63`).
        assert!(rt.sandbox_runtime_handler(&mk(Some("nope"))).await.is_err());
    }

    /// `getPodRuntimeHandler` (`kuberuntime_container.go:177-190`) and
    /// `EnsureImageExists` (`images/image_manager.go:194`).
    #[tokio::test]
    async fn image_runtime_handler_follows_gate_and_pod_class() {
        use rusternetes_common::feature_gates::{with_feature, Feature};
        use rusternetes_common::resources::RuntimeClass;
        let storage = Arc::new(StorageBackend::new_memory());
        storage
            .create(
                &rusternetes_storage::build_key("runtimeclasses", None, "gvisor"),
                &RuntimeClass::new("gvisor", "runsc"),
            )
            .await
            .unwrap();
        let vm = rusternetes_kubelet::volumes::VolumeManager::new(
            "/tmp/rusternetes-test-vols".to_string(),
            Some(storage),
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
        );
        let rt = CriContainerRuntime::connect(
            "/tmp/rusternetes-test-missing-cri.sock",
            "/tmp/rusternetes-test-logs",
        )
        .await
        .unwrap()
        .with_volumes(vm);
        let mk = |rc: Option<&str>| {
            Pod::new(
                "p",
                PodSpec {
                    runtime_class_name: rc.map(str::to_string),
                    ..Default::default()
                },
            )
        };
        {
            // Gate off (default): always "" (no node-default fallback).
            let _g = with_feature(Feature::RuntimeClassInImageCriApi, false);
            assert_eq!(
                rt.pod_runtime_handler(Some(&mk(Some("gvisor"))))
                    .await
                    .unwrap(),
                ""
            );
        }
        let _g = with_feature(Feature::RuntimeClassInImageCriApi, true);
        assert_eq!(
            rt.pod_runtime_handler(Some(&mk(Some("gvisor"))))
                .await
                .unwrap(),
            "runsc"
        );
        // No class (or empty) passes "".
        assert_eq!(rt.pod_runtime_handler(Some(&mk(None))).await.unwrap(), "");
        assert_eq!(
            rt.pod_runtime_handler(Some(&mk(Some("")))).await.unwrap(),
            ""
        );
        assert_eq!(rt.pod_runtime_handler(None).await.unwrap(), "");
        // Missing class errors.
        assert!(rt
            .pod_runtime_handler(Some(&mk(Some("nope"))))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn pod_dns_empty_resolv_conf_uses_node_ip_family_for_localhost() {
        use rusternetes_common::resources::pod::PodSpec;
        // dns.go:431-447: --resolv-conf="" appends ::1 per IPv6 node IP and
        // 127.0.0.1 per IPv4 node IP.
        let rt = CriContainerRuntime::connect(
            "/tmp/rusternetes-test-missing-cri.sock",
            "/tmp/rusternetes-test-logs",
        )
        .await
        .unwrap()
        .with_resolv_conf("")
        .with_node_ips(vec!["fd00::1".to_string()]);
        let pod = Pod::new(
            "p",
            PodSpec {
                dns_policy: Some("Default".to_string()),
                ..Default::default()
            },
        );
        let dns = rt.pod_dns(&pod).await.unwrap();
        assert_eq!(dns.servers, vec!["::1".to_string()]);
    }

    #[tokio::test]
    async fn failed_post_start_hook_emits_event_and_returns_error() {
        use rusternetes_common::resources::pod::PodSpec;

        let storage = Arc::new(StorageBackend::new_memory());
        let runtime = CriContainerRuntime::connect(
            "/tmp/rusternetes-test-missing-cri.sock",
            "/tmp/rusternetes-test-logs",
        )
        .await
        .unwrap()
        .with_event_recorder(Arc::clone(&storage));

        let mut pod = Pod::new(
            "poststart-fail",
            PodSpec {
                containers: vec![rusternetes_common::resources::pod::Container {
                    name: "app".to_string(),
                    image: "busybox".to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            },
        );
        pod.metadata.namespace = Some("default".to_string());
        pod.metadata.uid = "poststart-fail-uid".to_string();
        let container = pod.spec.as_ref().unwrap().containers[0].clone();

        // The CRI socket is absent, so the best-effort kill fails silently — the
        // event + returned error contract is what matters here.
        let mut cri = runtime.cri.clone();
        let err = runtime
            .fail_post_start_hook(
                &mut cri,
                &pod,
                &container,
                "container-abc123",
                anyhow::anyhow!("exec hook exited 1"),
            )
            .await;

        // Returned error is the PostStartHookError callers propagate to fail the
        // start (upstream `ErrPostStartHook`).
        assert!(
            err.to_string().contains("PostStartHookError"),
            "must return PostStartHookError, got: {err}"
        );

        // A FailedPostStartHook Warning event was recorded, with the generic
        // message (upstream keeps the handler error out to avoid secret leaks).
        let obj = rusternetes_kubelet::events::container_object_reference(&pod, "app");
        let key = format!(
            "/registry/events/default/{}",
            Event::generate_name(&obj, rusternetes_kubelet::events::FAILED_POST_START_HOOK)
        );
        let ev: Event = storage
            .get(&key)
            .await
            .expect("FailedPostStartHook event must be recorded");
        assert_eq!(
            ev.reason,
            rusternetes_kubelet::events::FAILED_POST_START_HOOK
        );
        assert_eq!(
            ev.event_type,
            rusternetes_common::resources::EventType::Warning
        );
        assert_eq!(
            ev.message, "PostStartHook failed",
            "message must be generic (no handler output) so a hook can't leak secrets"
        );
        assert_eq!(
            ev.involved_object.field_path.as_deref(),
            Some("spec.containers{app}")
        );
    }

    /// Regression test for NC "[sig-node] InitContainer [NodeConformance]
    /// should not start app containers if init containers fail on a
    /// RestartAlways pod" (init_container.go:440).
    ///
    /// Scenario: pod with init1 (failing/retrying) + init2 (never started).
    /// `statuses_for` synthesises a `Waiting{"ContainerCreating"}` status for
    /// init2 because the CRI has never created it.  After
    /// `fix_not_started_init_waiting_reason` that must become
    /// `Waiting{"PodInitializing"}`, matching upstream's default
    /// (`kubelet_pods.go:2431-2433`).
    #[test]
    fn second_init_container_not_started_shows_pod_initializing() {
        // init1 is failing (CrashLoopBackOff), init2 was never submitted to the
        // CRI so `statuses_for` returns it as Waiting{"ContainerCreating"}.
        let mut statuses = vec![
            // init1: already has a real CRI-derived reason — must be unchanged.
            ContainerStatus {
                name: "init1".to_string(),
                ready: false,
                restart_count: 2,
                state: Some(ContainerState {
                    waiting: Some(ContainerStateWaiting {
                        reason: Some("CrashLoopBackOff".to_string()),
                        message: Some("back-off restarting failed container".to_string()),
                    }),
                    ..Default::default()
                }),
                last_state: Some(ContainerState {
                    terminated: Some(ContainerStateTerminated {
                        exit_code: 1,
                        signal: None,
                        reason: Some("Error".to_string()),
                        message: None,
                        started_at: None,
                        finished_at: None,
                        container_id: None,
                    }),
                    ..Default::default()
                }),
                ..waiting_status("init1")
            },
            // init2: synthesised by `waiting_status` — ContainerCreating must
            // be replaced with PodInitializing.
            waiting_status("init2"),
        ];

        // Before the fix, init2 has ContainerCreating — verify the pre-fix
        // state so the test documents the regression it catches.
        match &statuses[1].state {
            Some(ContainerState {
                waiting: Some(ContainerStateWaiting { reason, .. }),
                ..
            }) => {
                assert_eq!(
                    reason.as_deref(),
                    Some("ContainerCreating"),
                    "pre-fix: waiting_status produces ContainerCreating"
                );
            }
            _ => panic!("init2 should start as Waiting"),
        }

        fix_not_started_init_waiting_reason(&mut statuses);

        // After the fix: init1's CrashLoopBackOff reason is preserved.
        match &statuses[0].state {
            Some(ContainerState {
                waiting: Some(ContainerStateWaiting { reason, .. }),
                ..
            }) => {
                assert_eq!(
                    reason.as_deref(),
                    Some("CrashLoopBackOff"),
                    "init1 CrashLoopBackOff must not be changed"
                );
            }
            _ => panic!("init1 should still be Waiting/CrashLoopBackOff"),
        }

        // After the fix: init2 reports PodInitializing, not ContainerCreating.
        match &statuses[1].state {
            Some(ContainerState {
                waiting: Some(ContainerStateWaiting { reason, .. }),
                ..
            }) => {
                assert_eq!(
                    reason.as_deref(),
                    Some("PodInitializing"),
                    "init2 (never started) must be Waiting/PodInitializing, not ContainerCreating"
                );
            }
            _ => panic!("init2 should be Waiting/PodInitializing after fix"),
        }
    }

    /// Regression test: an exec postStart lifecycle handler MUST return an error
    /// within `HANDLER_TIMEOUT` when the CRI runtime is unresponsive (accepts the
    /// TCP connection but never sends a gRPC response).
    ///
    /// Without an outer `tokio::time::timeout` guard in `run_lifecycle_handler`,
    /// `exec_sync` blocks indefinitely — `start_pod` hangs, and `CreateSync`
    /// times out after 300 s in the conformance suite, causing the NC test
    /// "[sig-node] Container Lifecycle Hook … should execute poststart exec hook
    /// properly [NodeConformance]" to fail.
    ///
    /// Upstream ref:
    ///   `pkg/kubelet/lifecycle/handlers.go:82` — the exec hook is called via
    ///   `hr.commandRunner.RunInContainer(ctx, containerID, handler.Exec.Command, 0)`;
    ///   the context carries the deadline that bounds the call.  Our Rust
    ///   equivalent is the outer `tokio::time::timeout(HANDLER_TIMEOUT, exec_sync)`.
    #[tokio::test(start_paused = true)]
    async fn exec_lifecycle_hook_returns_error_when_cri_hangs() {
        use rusternetes_common::resources::pod::ExecAction;
        use std::time::Duration;
        use tokio::io::AsyncReadExt;

        // Spawn a Unix socket server that accepts connections but never sends
        // any data back — simulates a containerd process stuck inside ExecSync
        // (e.g. waiting for a subprocess that never exits).
        let tmp = tempfile::tempdir().unwrap();
        let sock_path = tmp.path().join("hanging-cri.sock");

        let listener = tokio::net::UnixListener::bind(&sock_path).unwrap();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                // Drain whatever the client sends but never reply.
                let mut buf = vec![0u8; 4096];
                loop {
                    match stream.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(_) => {}
                    }
                }
            }
        });

        let runtime =
            CriContainerRuntime::connect(sock_path.to_str().unwrap(), "/tmp/rusternetes-test-logs")
                .await
                .unwrap();

        let (pod, container) = lifecycle_test_pod_and_container();
        let handler = rusternetes_common::resources::pod::LifecycleHandler {
            exec: Some(ExecAction {
                command: vec!["sh".to_string(), "-c".to_string(), "exit 0".to_string()],
            }),
            http_get: None,
            tcp_socket: None,
            sleep: None,
        };

        // Spawn the handler call so we can advance simulated time while it is
        // blocked on the unresponsive CRI socket.
        let handle = tokio::spawn(async move {
            runtime
                .run_lifecycle_handler(&pod, &container, "container-abc123", &handler)
                .await
        });

        // Yield enough times to let the spawned task connect to the socket and
        // reach the exec_sync await point before we advance the clock.
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }

        // Advance tokio's simulated time past HANDLER_TIMEOUT (60 s).
        // With the fix in place, the inner `tokio::time::timeout(HANDLER_TIMEOUT,
        // exec_sync)` fires here and the handler task returns Err.  Without the
        // fix there is no timer around exec_sync, so advancing time does not
        // unblock it and `handle.await` below hangs indefinitely (the test would
        // deadlock / time-out in CI).
        tokio::time::advance(Duration::from_secs(61)).await;

        // Give the task one more turn to process the fired timeout.
        for _ in 0..4 {
            tokio::task::yield_now().await;
        }

        let result = handle
            .await
            .expect("spawned task must complete after HANDLER_TIMEOUT");
        assert!(
            result.is_err(),
            "exec lifecycle handler must return Err when CRI is unresponsive for HANDLER_TIMEOUT"
        );
    }

    /// Port of `TestToKubeContainerImageVolumes`
    /// (`kuberuntime_manager_test.go:3717-3783`).
    #[test]
    fn to_kube_container_image_volumes_cases() {
        use rusternetes_common::resources::pod::VolumeMount;
        let spec1 = v1::ImageSpec {
            image: "image-1".into(),
            ..Default::default()
        };
        let spec2 = v1::ImageSpec {
            image: "image-2".into(),
            ..Default::default()
        };
        let container = |names: &[&str]| Container {
            volume_mounts: Some(
                names
                    .iter()
                    .map(|n| VolumeMount {
                        name: n.to_string(),
                        mount_path: format!("/{n}"),
                        read_only: None,
                        sub_path: None,
                        sub_path_expr: None,
                        mount_propagation: None,
                        recursive_read_only: None,
                    })
                    .collect(),
            ),
            ..Default::default()
        };

        // "empty volumes"
        let none = ImageVolumePulls::new();
        assert!(
            to_kube_container_image_volumes(&none, &Container::default())
                .unwrap()
                .is_empty()
        );

        // "multiple volumes"
        let mut pulls = ImageVolumePulls::new();
        pulls.insert("volume-1".into(), Ok(spec1.clone()));
        pulls.insert("volume-2".into(), Ok(spec2.clone()));
        let got =
            to_kube_container_image_volumes(&pulls, &container(&["volume-1", "volume-2"])).unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(got["volume-1"], spec1);
        assert_eq!(got["volume-2"], spec2);

        // "not matching volume"
        let mut other = ImageVolumePulls::new();
        other.insert("different".into(), Ok(spec1.clone()));
        assert!(
            to_kube_container_image_volumes(&other, &container(&["volume-1"]))
                .unwrap()
                .is_empty()
        );

        // "error in pull result": the container that mounts it fails, with
        // the pull error verbatim; a container that does not still starts.
        let mut failed = ImageVolumePulls::new();
        failed.insert(
            "volume-1".into(),
            Err(Arc::new(anyhow::anyhow!("pull failed"))),
        );
        let err = to_kube_container_image_volumes(&failed, &container(&["volume-1"])).unwrap_err();
        assert_eq!(err.to_string(), "pull failed");
        assert!(to_kube_container_image_volumes(&failed, &container(&["x"]))
            .unwrap()
            .is_empty());
    }

    /// `getImageVolumes`' `"multiple volumes but one failed to pull"` case
    /// (`kuberuntime_manager_test.go:3846-3855`): the Never-policy failure
    /// stays the typed `ErrImageNeverPull` the kubelet maps to a waiting
    /// reason, with upstream's message.
    #[test]
    fn a_never_pull_failure_keeps_its_type_and_message() {
        use rusternetes_common::resources::pod::VolumeMount;
        let mut pulls = ImageVolumePulls::new();
        pulls.insert(
            "volume-2".into(),
            Err(Arc::new(anyhow::Error::new(
                rusternetes_kubelet::lifecycle::ImageNeverPullError {
                    image: "image".into(),
                },
            ))),
        );
        let c = Container {
            volume_mounts: Some(vec![VolumeMount {
                name: "volume-2".into(),
                mount_path: "/v".into(),
                read_only: None,
                sub_path: None,
                sub_path_expr: None,
                mount_propagation: None,
                recursive_read_only: None,
            }]),
            ..Default::default()
        };
        let err = to_kube_container_image_volumes(&pulls, &c).unwrap_err();
        assert_eq!(
            err.to_string(),
            r#"Container image "image" is not present with pull policy of Never"#
        );
        assert_eq!(
            rusternetes_kubelet::lifecycle::reason_from_anyhow(&err),
            Some("ErrImageNeverPull")
        );
    }

    /// An image volume's `GetPath` is empty (`image.go:83`) so its entry in
    /// the volume-path map must not be taken for a volume root that every
    /// absolute subPath mount "lives under".
    #[test]
    fn an_empty_volume_root_is_not_a_sub_path_root() {
        let tmp = tempfile::tempdir().unwrap();
        let missing = tmp.path().join("not-created");
        let host_paths = std::collections::HashMap::from([("img".to_string(), String::new())]);
        let cfg = v1::ContainerConfig {
            mounts: vec![v1::Mount {
                host_path: missing.to_string_lossy().to_string(),
                ..Default::default()
            }],
            ..Default::default()
        };
        create_sub_path_dirs("c", &host_paths, &cfg);
        assert!(!missing.exists());
    }
}
